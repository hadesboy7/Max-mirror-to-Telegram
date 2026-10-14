"""Постоянное хранение состояния обработки сообщений (SQLite).

ЗАЧЕМ. До этого модуля множества _seen/_forwarded жили только в оперативной
памяти. Любой перезапуск (а они штатные: Restart=always, сторож по пульсу,
memclean при нехватке памяти, _die при смерти браузера) стирал их, и прайминг
помечал всё видимое как «уже виденное» БЕЗ пересылки. Сообщения, пришедшие за
окно простоя, терялись безвозвратно.

МОДЕЛЬ СОСТОЯНИЙ одного обнаруженного события:

    detected ──► sending ──► delivered        (успех)
        ▲           │
        │           └──────► failed ──┐       (временная ошибка)
        └───────────────────────────────┘     (повтор с нарастающей задержкой)

Сообщение считается доставленным ТОЛЬКО после подтверждения Telegram
(mark_delivered вызывается после успешного await отправки).

ГАРАНТИИ И ИХ ГРАНИЦЫ:
  • «не продублировать» — таблица messages живёт между перезапусками, поэтому
    повторное чтение одного и того же DOM не создаёт второй записи
    (INSERT OR IGNORE по первичному ключу sig).
  • «не потерять» — запись не удаляется и не помечается delivered, пока
    Telegram не подтвердил отправку; при перезапуске недоставленные
    возвращаются в очередь.
  • Абсолютной идемпотентности отправки Telegram Bot API не даёт. Если процесс
    умер РОВНО между успешной отправкой и записью delivered, запись останется в
    состоянии sending. Политика для таких записей настраивается
    (RESEND_STUCK_SENDING): по умолчанию повторяем, потому что потеря сообщения
    для владельца хуже дубликата. Это осознанный компромисс, а не гарантия.
"""

import json
import os
import sqlite3
import time

SCHEMA_VERSION = 2

# Политика для записей, застрявших в sending (процесс умер во время отправки):
#   True  — повторить отправку (риск дубликата, потери нет)
#   False — считать доставленным (риск потери, дубликата нет)
RESEND_STUCK_SENDING = os.environ.get("RESEND_STUCK_SENDING", "1") == "1"

# Нарастающая задержка повтора: 30с, 60с, 120с … но не больше часа
RETRY_BASE_SEC = int(os.environ.get("RETRY_BASE_SEC", "30"))
RETRY_MAX_SEC = int(os.environ.get("RETRY_MAX_SEC", "3600"))
# После такого числа неудач перестаём спамить попытками и зовём человека
RETRY_GIVEUP_AT = int(os.environ.get("RETRY_GIVEUP_AT", "20"))

_DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    -- Ключ СОСТАВНОЙ: одно и то же сообщение порождает до трёх событий, и
    -- удаление несёт подпись ИСХОДНОГО текста — та же, что у появления.
    -- Если ключом сделать только sig, удаления молча теряются.
    event_id     TEXT PRIMARY KEY,   -- kind + ':' + sig
    sig          TEXT NOT NULL,      -- контентная подпись (автор|текст-или-медиа|мета|#n)
    kind         TEXT NOT NULL,      -- 'new' | 'edit' | 'delete'
    author       TEXT NOT NULL DEFAULT '',
    text         TEXT NOT NULL DEFAULT '',
    meta_time    TEXT NOT NULL DEFAULT '',
    media_json   TEXT,               -- медиа как есть, чтобы доставить после перезапуска
    reply_json   TEXT,               -- цитата (реплай)
    state        TEXT NOT NULL,      -- detected | sending | delivered | failed | giveup
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    first_seen   INTEGER NOT NULL,
    last_try     INTEGER NOT NULL DEFAULT 0,
    delivered_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_messages_state ON messages(state);
CREATE INDEX IF NOT EXISTS idx_messages_sig   ON messages(sig);
CREATE INDEX IF NOT EXISTS idx_messages_seen  ON messages(first_seen);

-- Состояние доставки ОТДЕЛЬНО НА КАЖДЫЙ ЧАТ.
-- Без этой таблицы успех в одном чате закрывал запись целиком, и второй чат
-- сообщение не получал никогда (дефект подтверждён тестом).
CREATE TABLE IF NOT EXISTS deliveries (
    event_id     TEXT NOT NULL,
    chat_id      INTEGER NOT NULL,
    state        TEXT NOT NULL,      -- detected | sending | delivered | failed | giveup
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    last_try     INTEGER NOT NULL DEFAULT 0,
    delivered_at INTEGER,
    PRIMARY KEY (event_id, chat_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_state ON deliveries(state);

-- Снимок видимого окна прошлого опроса. Нужен, чтобы classify() после
-- перезапуска сравнивал с реальным прошлым состоянием, а не с пустотой.
CREATE TABLE IF NOT EXISTS window_snapshot (
    pos    INTEGER PRIMARY KEY,
    sig    TEXT NOT NULL,
    msgkey TEXT NOT NULL,
    author TEXT NOT NULL DEFAULT '',
    text   TEXT NOT NULL DEFAULT ''
);
"""


class StateStore:
    """Хранилище состояния. Все методы синхронные и быстрые (локальный SQLite)."""

    def __init__(self, path: str):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        if d:
            os.makedirs(d, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        # WAL: переживает падение процесса; NORMAL — разумный компромисс
        # скорости и надёжности (при SIGKILL возможна потеря последней
        # транзакции, что хуже всего даёт повторную отправку, не потерю).
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(_DDL)
        cur = self.db.execute("SELECT value FROM meta WHERE key='schema_version'")
        row = cur.fetchone()
        if row is None:
            self.db.execute("INSERT INTO meta(key,value) VALUES('schema_version',?)",
                            (str(SCHEMA_VERSION),))

    # ------------------------------------------------------------------ служебное
    def close(self):
        try:
            self.db.close()
        except Exception:
            pass

    def _get_meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def _set_meta(self, key, value):
        self.db.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (key, str(value)))

    # ------------------------------------------------- первый запуск / прайминг
    def is_bootstrapped(self) -> bool:
        """Был ли уже первый «тихий» прайминг. Если нет — историю не пересылаем."""
        return self._get_meta("bootstrapped") == "1"

    def mark_bootstrapped(self):
        self._set_meta("bootstrapped", 1)

    # --------------------------------------------------------------- обнаружение
    def record_detected(self, kind: str, msg: dict, targets=()) -> bool:
        """Регистрирует обнаруженное событие и очередь доставки по каждому чату.

        True — событие новое (надо доставить), False — уже знаем (повторное
        чтение DOM; дубликат не создаём)."""
        now = int(time.time())
        cur = self.db.execute(
            "INSERT OR IGNORE INTO messages"
            "(event_id,sig,kind,author,text,meta_time,media_json,reply_json,state,first_seen) "
            "VALUES(?,?,?,?,?,?,?,?, 'detected', ?)",
            (f"{kind}:{msg['sig']}", msg["sig"], kind, msg.get("author", ""), msg.get("text", ""),
             msg.get("meta_time", ""),
             json.dumps(msg["media"], ensure_ascii=False) if msg.get("media") else None,
             json.dumps(msg["reply"], ensure_ascii=False) if msg.get("reply") else None,
             now))
        new = cur.rowcount > 0
        # очередь на каждого получателя (идемпотентно)
        eid = f"{kind}:{msg['sig']}"
        for chat in targets:
            self.db.execute(
                "INSERT OR IGNORE INTO deliveries(event_id,chat_id,state) "
                "VALUES(?,?, 'detected')", (eid, int(chat)))
        return new

    def record_silent(self, kind: str, msg: dict, targets=()) -> bool:
        """Регистрирует сообщение как УЖЕ доставленное, без отправки.
        Используется только при первом прайминге, чтобы не слить историю в Telegram."""
        now = int(time.time())
        cur = self.db.execute(
            "INSERT OR IGNORE INTO messages"
            "(event_id,sig,kind,author,text,meta_time,state,first_seen,delivered_at) "
            "VALUES(?,?,?,?,?,?, 'delivered', ?, ?)",
            (f"{kind}:{msg['sig']}", msg["sig"], kind, msg.get("author", ""), msg.get("text", ""),
             msg.get("meta_time", ""), now, now))
        eid = f"{kind}:{msg['sig']}"
        for chat in targets:
            self.db.execute(
                "INSERT OR IGNORE INTO deliveries"
                "(event_id,chat_id,state,delivered_at) VALUES(?,?, 'delivered', ?)",
                (eid, int(chat), now))
        return cur.rowcount > 0

    def known(self, sig: str) -> bool:
        return self.db.execute("SELECT 1 FROM messages WHERE sig=?",
                               (sig,)).fetchone() is not None

    def is_delivered(self, sig: str) -> bool:
        """Дошло ли это сообщение ХОТЯ БЫ до одного чата.

        Именно такая семантика нужна classify(): правку или удаление имеет смысл
        пересылать только если пользователь исходное сообщение уже видел."""
        row = self.db.execute(
            "SELECT 1 FROM deliveries d JOIN messages m ON m.event_id=d.event_id "
            "WHERE m.sig=? AND d.state='delivered' LIMIT 1", (sig,)).fetchone()
        return row is not None

    def is_event_delivered(self, kind: str, sig: str) -> bool:
        """Было ли доставлено КОНКРЕТНОЕ событие (нужно для удалений)."""
        row = self.db.execute("SELECT 1 FROM messages WHERE event_id=? AND "
                              "state='delivered' LIMIT 1",
                              (f"{kind}:{sig}",)).fetchone()
        return row is not None

    # ------------------------------------------------------------------ доставка
    def claim_pending(self, limit: int = 20) -> list:
        """События, у которых есть хотя бы один НЕдоставленный получатель.

        Для каждого события отдаёт `targets` — только те чаты, которые ещё не
        получили сообщение. Чат, который уже получил, повторно не беспокоим.
        """
        now = int(time.time())
        rows = self.db.execute(
            "SELECT DISTINCT m.* FROM messages m JOIN deliveries d "
            "  ON d.event_id = m.event_id "
            "WHERE d.state IN ('detected','failed','sending') "
            "ORDER BY m.first_seen LIMIT ?", (limit * 4,)).fetchall()
        out = []
        for r in rows:
            targets = []
            for d in self.db.execute(
                    "SELECT chat_id,state,attempts,last_try FROM deliveries "
                    "WHERE event_id=? AND state IN ('detected','failed')",
                    (r["event_id"],)):
                if d["state"] == "failed":
                    delay = min(RETRY_BASE_SEC * (2 ** max(0, d["attempts"] - 1)),
                                RETRY_MAX_SEC)
                    if now - d["last_try"] < delay:
                        continue                  # повтор для этого чата ещё не дозрел
                targets.append(d["chat_id"])
            if not targets:
                continue
            self.db.execute(
                "UPDATE deliveries SET state='sending', last_try=? "
                "WHERE event_id=? AND chat_id IN (%s)"
                % ",".join("?" * len(targets)),
                (now, r["event_id"], *targets))
            msg = self._row_to_msg(r)
            msg["targets"] = targets
            out.append(msg)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _row_to_msg(r) -> dict:
        return {
            "event_id": r["event_id"],
            "sig": r["sig"], "kind": r["kind"], "author": r["author"],
            "text": r["text"], "meta_time": r["meta_time"],
            "media": json.loads(r["media_json"]) if r["media_json"] else None,
            "reply": json.loads(r["reply_json"]) if r["reply_json"] else None,
            "first_seen": r["first_seen"],
            # attempts здесь больше нет: попытки считаются ПО КАЖДОМУ чату
            # в таблице deliveries
        }

    def release(self, event_id: str, chats) -> None:
        """Возвращает заявки из sending обратно в detected БЕЗ штрафа.

        Нужно, когда цикл доставки исчерпал бюджет времени: иначе заявки
        остались бы в sending и повторились только после перезапуска."""
        for chat in chats:
            self.db.execute("UPDATE deliveries SET state='detected' "
                            "WHERE event_id=? AND chat_id=? AND state='sending'",
                            (event_id, int(chat)))
        self._sync_event_state(event_id)

    def mark_delivered(self, event_id: str, chat_id: int):
        """Вызывается ТОЛЬКО после подтверждённой отправки в КОНКРЕТНЫЙ чат."""
        now = int(time.time())
        self.db.execute("UPDATE deliveries SET state='delivered', delivered_at=?, "
                        "last_error=NULL WHERE event_id=? AND chat_id=?",
                        (now, event_id, int(chat_id)))
        self._sync_event_state(event_id)

    def _sync_event_state(self, event_id: str):
        """Сводное состояние события = по его получателям.
        delivered — когда у всех получателей дело закрыто (доставлено или отказ)."""
        rows = self.db.execute("SELECT state FROM deliveries WHERE event_id=?",
                               (event_id,)).fetchall()
        if not rows:
            return
        states = [r["state"] for r in rows]
        if all(st in ("delivered", "giveup") for st in states):
            now = int(time.time())
            self.db.execute("UPDATE messages SET state='delivered', delivered_at=? "
                            "WHERE event_id=?", (now, event_id))
        elif any(st == "failed" for st in states):
            self.db.execute("UPDATE messages SET state='failed' WHERE event_id=?",
                            (event_id,))
        else:
            self.db.execute("UPDATE messages SET state='sending' WHERE event_id=?",
                            (event_id,))

    def mark_failed(self, event_id: str, chat_id: int, error: str):
        """Временная неудача по КОНКРЕТНОМУ чату: повтор с нарастающей задержкой."""
        now = int(time.time())
        row = self.db.execute("SELECT attempts FROM deliveries WHERE event_id=? "
                              "AND chat_id=?", (event_id, int(chat_id))).fetchone()
        attempts = (row["attempts"] if row else 0) + 1
        state = "giveup" if attempts >= RETRY_GIVEUP_AT else "failed"
        self.db.execute("UPDATE deliveries SET state=?, attempts=?, last_error=?, "
                        "last_try=? WHERE event_id=? AND chat_id=?",
                        (state, attempts, str(error)[:300], now, event_id, int(chat_id)))
        self._sync_event_state(event_id)
        return state

    def recover_stuck_sending(self) -> int:
        """При старте: записи, застрявшие в sending (процесс умер во время
        отправки). Политика в RESEND_STUCK_SENDING. Возвращает их число."""
        rows = self.db.execute(
            "SELECT event_id, chat_id FROM deliveries WHERE state='sending'").fetchall()
        if not rows:
            return 0
        if RESEND_STUCK_SENDING:
            self.db.execute("UPDATE deliveries SET state='detected' WHERE state='sending'")
        else:
            now = int(time.time())
            self.db.execute("UPDATE deliveries SET state='delivered', delivered_at=? "
                            "WHERE state='sending'", (now,))
        for r in rows:
            self._sync_event_state(r["event_id"])
        return len(rows)

    def pending_count(self) -> int:
        """Сколько пар (событие, чат) ещё не доставлено."""
        row = self.db.execute("SELECT COUNT(*) c FROM deliveries "
                              "WHERE state IN ('detected','sending','failed')").fetchone()
        return row["c"]

    def pending_by_chat(self) -> dict:
        """Очередь в разрезе чатов — для heartbeat и диагностики."""
        return {r["chat_id"]: r["c"] for r in self.db.execute(
            "SELECT chat_id, COUNT(*) c FROM deliveries "
            "WHERE state IN ('detected','sending','failed') GROUP BY chat_id")}

    def chat_delivered(self, kind: str, sig: str, chat_id: int) -> bool:
        """Получил ли КОНКРЕТНЫЙ чат это событие."""
        row = self.db.execute(
            "SELECT 1 FROM deliveries WHERE event_id=? AND chat_id=? "
            "AND state='delivered' LIMIT 1",
            (f"{kind}:{sig}", int(chat_id))).fetchone()
        return row is not None

    # ---------------------------------------------- снимок окна для classify()
    def save_window(self, msgs: list):
        """Сохраняет видимое окно последнего опроса (одной транзакцией)."""
        self.db.execute("BEGIN")
        try:
            self.db.execute("DELETE FROM window_snapshot")
            self.db.executemany(
                "INSERT INTO window_snapshot(pos,sig,msgkey,author,text) VALUES(?,?,?,?,?)",
                [(i, m["sig"], m.get("key", ""), m.get("author", ""), m.get("text", ""))
                 for i, m in enumerate(msgs)])
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def load_window(self) -> list:
        """Возвращает сохранённое окно в формате, который понимает classify()."""
        rows = self.db.execute(
            "SELECT sig,msgkey,author,text FROM window_snapshot ORDER BY pos").fetchall()
        return [{"sig": r["sig"], "key": r["msgkey"], "author": r["author"],
                 "text": r["text"], "media": None, "reply": None} for r in rows]

    # -------------------------------------------- загрузка множеств для classify
    def load_seen(self, limit: int) -> list:
        """Подписи последних known-сообщений (аналог прежнего _seen)."""
        rows = self.db.execute(
            "SELECT DISTINCT sig FROM messages ORDER BY first_seen DESC LIMIT ?",
            (limit,)).fetchall()
        return [r["sig"] for r in rows]

    def load_forwarded(self, limit: int) -> list:
        """Подписи доставленных (аналог прежнего _forwarded)."""
        rows = self.db.execute(
            "SELECT DISTINCT m.sig FROM messages m JOIN deliveries d "
            "  ON d.event_id=m.event_id "
            "WHERE d.state='delivered' ORDER BY m.first_seen DESC LIMIT ?",
            (limit,)).fetchall()
        return [r["sig"] for r in rows]

    # ------------------------------------------------------------------ прочее
    def prune(self, keep: int):
        """Ограничивает размер базы: удаляет САМЫЕ СТАРЫЕ доставленные записи.
        Недоставленные не удаляются никогда."""
        self.db.execute(
            "DELETE FROM messages WHERE state='delivered' AND event_id NOT IN ("
            "  SELECT event_id FROM messages ORDER BY first_seen DESC LIMIT ?)", (keep,))
        # строки доставки без события больше не нужны
        self.db.execute("DELETE FROM deliveries WHERE event_id NOT IN "
                        "(SELECT event_id FROM messages)")

    def stats(self) -> dict:
        out = {}
        for r in self.db.execute("SELECT state, COUNT(*) c FROM messages GROUP BY state"):
            out[r["state"]] = r["c"]
        out["window"] = self.db.execute(
            "SELECT COUNT(*) c FROM window_snapshot").fetchone()["c"]
        out["bootstrapped"] = self.is_bootstrapped()
        out["по_чатам"] = self.pending_by_chat()
        return out
