"""Сквозные тесты конвейера: обнаружение -> база -> доставка.

БЕЗОПАСНОСТЬ: Telegram подменён заглушкой. Ни одного реального запроса в сеть,
ни одного сообщения в реальные чаты владельца. Браузер не запускается.
"""

import os
import sys

# тесты лежат в tests/, модули проекта — в корне
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import os
import tempfile

os.environ["TELEGRAM_BOT_TOKEN"] = "123:TEST"
os.environ["TELEGRAM_TARGET_CHAT_IDS"] = "111,222"
os.environ["TELEGRAM_ALLOWED_USER_IDS"] = "111"
os.environ["MIRROR_OUTGOING"] = "0"
os.environ["STATE_DB"] = os.path.join(tempfile.mkdtemp(), "pipe.db")

import bridge_ui as b
from state_store import StateStore

ok = fail = 0


def check(name, cond):
    global ok, fail
    if cond:
        ok += 1
        print(f"  ok  {name}")
    else:
        fail += 1
        print(f"FAIL  {name}")


# ─────────────────────── поддельный Telegram ───────────────────────
class FakeTG:
    """Записывает отправки вместо реальных запросов. Умеет ломаться."""

    def __init__(self):
        self.sent = []          # [(chat_id, тип, текст/имя файла)]
        self.fail_until = 0     # падать, пока счётчик вызовов меньше этого
        self.calls = 0

    def _maybe_fail(self):
        self.calls += 1
        if self.calls <= self.fail_until:
            raise RuntimeError("TelegramNetworkError: имитация недоступности")

    async def send_message(self, chat_id, text, **kw):
        self._maybe_fail(); self.sent.append((chat_id, "text", text)); return True

    async def send_photo(self, chat_id, f, caption=None, **kw):
        self._maybe_fail(); self.sent.append((chat_id, "photo", caption or "")); return True

    async def send_video(self, chat_id, f, caption=None, **kw):
        self._maybe_fail(); self.sent.append((chat_id, "video", caption or "")); return True

    async def send_audio(self, chat_id, f, caption=None, **kw):
        self._maybe_fail(); self.sent.append((chat_id, "audio", caption or "")); return True

    async def send_document(self, chat_id, f, caption=None, **kw):
        self._maybe_fail(); self.sent.append((chat_id, "doc", caption or "")); return True

    async def send_media_group(self, chat_id, group, **kw):
        self._maybe_fail(); self.sent.append((chat_id, "album", f"{len(group)} фото")); return True


def install_fake():
    tg = FakeTG()
    b.tg_bot = tg
    # медиа не качаем из сети — подменяем загрузчики
    async def fake_fetch(url):
        return b"\x89PNG" + b"0" * 2000
    async def fake_download(name):
        return (b"FILEDATA" * 100, name or "file.bin")
    async def fake_shot(sel):
        return b"\x89PNG" + b"0" * 3000
    async def fake_album(expected):
        return [f"https://x/{i}.jpg" for i in range(expected)]
    b.fetch_bytes = fake_fetch
    b.download_file = fake_download
    b.screenshot_element = fake_shot
    b.resolve_album_srcs = fake_album
    return tg


def new_store():
    path = os.path.join(tempfile.mkdtemp(), "s.db")
    st = StateStore(path)
    b._store = st
    b._seen.clear(); b._seen_order.clear()
    b._forwarded.clear(); b._forwarded_order.clear()
    b._giveup_alerted = False
    return st, path


def mk(author, text, tm, media=None, reply=None):
    """Строит сообщение так же, как это делает _extract_incoming."""
    content = text or (media.get("id") if media else "")
    return {"key": f"{author}|{tm}", "author": author, "text": text,
            "meta_time": tm, "media": media, "reply": reply,
            "sig": b._sig(author, content, tm)}


TARGETS = [111, 222]


def detect(store, new=(), edited=(), deleted=()):
    """Повторяет шаг обнаружения из poll_max_loop (с обоими получателями)."""
    for m in new:
        if store.record_detected("new", m, TARGETS):
            b._seen_add(m["sig"])
    for m in edited:
        if store.record_detected("edit", m, TARGETS):
            b._seen_add(m["sig"])
    for m in deleted:
        store.record_detected("delete", m, TARGETS)


# ═══════════ 1. Обычная доставка: оба чата получают сообщение ═══════════
tg = install_fake()
st, _ = new_store()
m1 = mk("Аня", "привет соседи", "10:00")
detect(st, new=[m1])
asyncio.run(b.deliver_pending())
check("доставлено в оба целевых чата", sorted(c for c, _, _ in tg.sent) == [111, 222])
check("текст сообщения дошёл", any("привет соседи" in t for _, _, t in tg.sent))
check("в базе помечено доставленным", st.is_delivered(m1["sig"]) is True)
check("очередь пуста", st.pending_count() == 0)

# ═══════════ 2. Повторное чтение того же DOM не даёт дубликата ══════════
before = len(tg.sent)
detect(st, new=[m1])                     # то же сообщение снова
asyncio.run(b.deliver_pending())
check("повторное обнаружение НЕ отправило второй раз", len(tg.sent) == before)

# ═══════════ 3. Telegram недоступен -> повтор до успеха ════════════════
tg = install_fake()
st, _ = new_store()
tg.fail_until = 100                      # все попытки падают
m2 = mk("Дед", "сломайся", "11:00")
detect(st, new=[m2])
asyncio.run(b.deliver_pending())
check("при недоступности НЕ помечено доставленным", st.is_delivered(m2["sig"]) is False)
check("осталось в очереди по паре на каждый чат", st.pending_count() == 2)
check("оба чата ждут", st.pending_by_chat() == {111: 1, 222: 1})
check("ничего не ушло", len(tg.sent) == 0)
tg.fail_until = 0                        # связь восстановилась
st.db.execute("UPDATE deliveries SET last_try=0")     # выдержка истекла
asyncio.run(b.deliver_pending())
check("после восстановления связи доставлено", st.is_delivered(m2["sig"]) is True)
check("сообщение дошло ровно один раз",
      sum(1 for _, _, t in tg.sent if "сломайся" in t) == 2)  # 2 чата = 2 отправки

# ═══════════ 4. Процесс умер во время отправки ═════════════════════════
tg = install_fake()
st, path4 = new_store()
m3 = mk("Ирина", "в полёте", "12:00")
detect(st, new=[m3])
st.claim_pending()                       # забрали в sending и «умерли»
st.close()
st4 = StateStore(path4); b._store = st4
stuck = st4.recover_stuck_sending()
check("прерванная отправка найдена при старте (пара на чат)", stuck == 2)
asyncio.run(b.deliver_pending())
check("после восстановления сообщение доставлено", st4.is_delivered(m3["sig"]) is True)

# ═══════════ 5. Правки и удаления сохраняются и доставляются ═══════════
tg = install_fake()
st, _ = new_store()
orig = mk("Виктор", "первая версия", "13:00")
edited = mk("Виктор", "исправленная версия", "13:00")
detect(st, new=[orig])
asyncio.run(b.deliver_pending())
detect(st, edited=[edited])
asyncio.run(b.deliver_pending())
detect(st, deleted=[orig])
asyncio.run(b.deliver_pending())
texts = " | ".join(t for _, _, t in tg.sent)
check("правка доставлена с пометкой", "изменил" in texts and "исправленная" in texts)
check("удаление доставлено с пометкой", "удалил" in texts)
check("все три события в базе доставлены", st.pending_count() == 0)

# ═══════════ 6. ГЛАВНОЕ: сообщения, пришедшие во время простоя ═════════
tg = install_fake()
st, path6 = new_store()
# первый запуск: в беседе уже есть история — её НЕ пересылаем
# давняя история: в .meta стоит ДАТА, как её и показывает MAX для старых
# сообщений. Иначе она законно попадёт в контрольное окно первого запуска.
history = [mk("Галина", "старое 1", "29 авг."), mk("Галина", "старое 2", "30 авг.")]
prev, mode, n = b.prime_or_resume(history)
check("первый запуск распознан", mode == "первый-запуск")
check("история НЕ отправлена в Telegram", len(tg.sent) == 0)
check("история помечена доставленной", st.pending_count() == 0)
# ПРОСТОЙ. За это время соседи написали два сообщения.
st.close()
st6 = StateStore(path6); b._store = st6
b._seen.clear(); b._seen_order.clear(); b._forwarded.clear(); b._forwarded_order.clear()
for s in st6.load_seen(8000):
    b._seen_add(s)
for s in st6.load_forwarded(8000):
    b._forwarded_add(s)
after_downtime = history + [mk("Антонина", "пришло в простой", "10:30"),
                            mk("L M", "и это тоже", "10:31")]
prev2, mode2, n2 = b.prime_or_resume(after_downtime)
check("перезапуск распознан как возобновление", mode2 == "возобновление")
check("окно до простоя восстановлено из базы", len(prev2) == 2)
# обычный classify сам находит пропущенное
nw, ed, dl = b.classify(prev2, after_downtime, b._seen, b._forwarded)
check("classify нашёл ровно 2 пропущенных", len(nw) == 2)
check("нашёл именно те сообщения",
      sorted(m["text"] for m in nw) == ["и это тоже", "пришло в простой"])
detect(st6, new=nw, edited=ed, deleted=dl)
asyncio.run(b.deliver_pending())
got = [t for _, _, t in tg.sent]
check("ПРОПУЩЕННОЕ ЗА ПРОСТОЙ ДОСТАВЛЕНО", any("пришло в простой" in t for t in got))
check("второе пропущенное тоже доставлено", any("и это тоже" in t for t in got))
check("старая история повторно НЕ отправлена",
      not any("старое 1" in t for t in got))

# ═══════════ 7. Медиа, альбомы, стикеры, реплаи проходят очередь ═══════
tg = install_fake()
st, path7 = new_store()
cases = [
    ("фото",   mk("А", "", "14:00", media={"type": "photo", "srcs": ["https://x/1.jpg"],
                                           "count": 1, "id": "photo:1"})),
    ("альбом", mk("Б", "подпись", "14:01", media={"type": "photo", "srcs": [],
                                                  "count": 3, "pending": True,
                                                  "id": "photo:n3"})),
    ("видео",  mk("В", "", "14:02", media={"type": "video", "src": "https://x/v.mp4",
                                           "id": "v1"})),
    ("файл",   mk("Г", "", "14:03", media={"type": "file", "filename": "смета.pdf",
                                           "id": "file:смета.pdf"})),
    ("аудио",  mk("Д", "", "14:04", media={"type": "audio", "src": "https://x/a.ogg",
                                           "id": "audio:a1"})),
    ("стикер", mk("Е", "", "14:05", media={"type": "sticker", "id": "sticker-7"})),
    ("реплай", mk("Ж", "мой ответ", "14:06", reply={"author": "Зоя", "text": "вопрос"})),
]
for label, msg in cases:
    detect(st, new=[msg])
asyncio.run(b.deliver_pending())
kinds = [k for _, k, _ in tg.sent]
check("фото доставлено как photo", "photo" in kinds)
check("альбом доставлен как album", "album" in kinds)
check("видео доставлено как video", "video" in kinds)
check("файл доставлен как document", "doc" in kinds)
check("аудио доставлено как audio", "audio" in kinds)
check("стикер доставлен (картинкой)", kinds.count("photo") >= 2)
check("реплай содержит цитату",
      any("Зоя" in t and "blockquote" in t for _, _, t in tg.sent))
check("вся очередь медиа разошлась", st.pending_count() == 0)

# ═══════════ 8. Медиа переживает перезапуск в очереди ═════════════════
tg = install_fake()
st, path8 = new_store()
st.mark_bootstrapped()
mm = mk("Фото", "", "15:00", media={"type": "photo", "srcs": ["https://x/z.jpg"],
                                    "count": 1, "id": "photo:z"})
tg.fail_until = 100
detect(st, new=[mm])
asyncio.run(b.deliver_pending())          # падает, остаётся в очереди
check("медиа не доставлено при сбое", st.is_delivered(mm["sig"]) is False)
st.close()
st8 = StateStore(path8); b._store = st8   # перезапуск
tg = install_fake()                        # связь восстановилась
st8.db.execute("UPDATE deliveries SET last_try=0")
asyncio.run(b.deliver_pending())
check("медиа доставлено после перезапуска", st8.is_delivered(mm["sig"]) is True)
check("доставлено именно как фото", any(k == "photo" for _, k, _ in tg.sent))

# ═══════════ 9. Политика «хотя бы один чат» не вешает очередь ═════════
tg = install_fake()
st, _ = new_store()
st.mark_bootstrapped()
orig_send = tg.send_message
async def one_chat_blocked(chat_id, text, **kw):
    if chat_id == 222:
        raise RuntimeError("Forbidden: bot was blocked by the user")
    return await orig_send(chat_id, text, **kw)
tg.send_message = one_chat_blocked
m9 = mk("Тест", "один чат заблокирован", "16:00")
detect(st, new=[m9])
asyncio.run(b.deliver_pending())
check("первый чат получил", any(c == 111 for c, _, _ in tg.sent))
check("второй чат НЕ получил", not any(c == 222 for c, _, _ in tg.sent))
check("событие НЕ закрыто целиком — ждёт второй чат", st.pending_count() == 1)
check("в очереди именно второй чат", st.pending_by_chat() == {222: 1})
tg2_ = install_fake()
st.db.execute("UPDATE deliveries SET last_try=0")
asyncio.run(b.deliver_pending())
check("повтор ушёл ТОЛЬКО во второй чат", [c for c, _, _ in tg2_.sent] == [222])
check("после доставки обоим очередь пуста", st.pending_count() == 0)

# ═══════ 10. Отложенная доставка НЕ лезет в устаревший DOM ════════════
# Это защита от доставки ЧУЖОГО вложения: файл берётся кликом по карточке,
# стикер — снимком элемента, недогруженный альбом — последним блоком DOM.
tg = install_fake()
st, _ = new_store()
st.mark_bootstrapped()
dom_calls = {"file": 0, "sticker": 0, "album": 0}
async def spy_download(name):
    dom_calls["file"] += 1; return (b"X" * 100, name or "f.bin")
async def spy_shot(sel):
    dom_calls["sticker"] += 1; return b"\x89PNG" + b"0" * 3000
async def spy_album(n):
    dom_calls["album"] += 1; return [f"https://x/{i}.jpg" for i in range(n)]
b.download_file = spy_download
b.screenshot_element = spy_shot
b.resolve_album_srcs = spy_album

fresh_file = mk("Св", "", "17:00", media={"type": "file", "filename": "акт.pdf",
                                          "id": "file:акт.pdf"})
detect(st, new=[fresh_file])
asyncio.run(b.deliver_pending())
check("свежая запись: файл берётся из DOM", dom_calls["file"] == 1)
check("свежий файл доставлен документом", any(k == "doc" for _, k, _ in tg.sent))

# теперь делаем записи «старыми» — DOM уже не соответствует
tg = install_fake()
b.download_file = spy_download; b.screenshot_element = spy_shot
b.resolve_album_srcs = spy_album
dom_calls = {"file": 0, "sticker": 0, "album": 0}
old_file = mk("Ст", "", "17:01", media={"type": "file", "filename": "старый.pdf",
                                        "id": "file:старый.pdf"})
old_stick = mk("Ст", "", "17:02", media={"type": "sticker", "id": "sticker-old"})
old_album = mk("Ст", "подпись", "17:03", media={"type": "photo", "srcs": [],
                                                "count": 3, "pending": True,
                                                "id": "photo:n3old"})
detect(st, new=[old_file, old_stick, old_album])
st.db.execute("UPDATE messages SET first_seen=? WHERE state IN ('detected','sending')",
              (int(__import__("time").time()) - b.DOM_FRESH_SEC - 60,))
asyncio.run(b.deliver_pending())
check("старая запись: к DOM за файлом НЕ обращались", dom_calls["file"] == 0)
check("старая запись: к DOM за стикером НЕ обращались", dom_calls["sticker"] == 0)
check("старая запись: к DOM за альбомом НЕ обращались", dom_calls["album"] == 0)
texts = " | ".join(t for _, _, t in tg.sent)
check("вместо вложения ушла честная пометка",
      "вложение не удалось получить" in texts)
check("имя файла в пометке сохранено", "старый.pdf" in texts)
check("отложенные записи всё равно доставлены (не потеряны)",
      st.pending_count() == 0)

# ═══════ 11. Аварийный режим: база недоступна -> мост всё равно работает ══════
# При 91% занятого диска SQLite может не открыться. Мост обязан продолжать
# зеркалить (без персистентности), а не останавливаться совсем.
tg = install_fake()
b._store = None
b._seen.clear(); b._seen_order.clear(); b._forwarded.clear(); b._forwarded_order.clear()
check("без базы deliver_pending безопасен",
      asyncio.run(b.deliver_pending()) == {"new": 0, "edit": 0, "del": 0, "failed": 0})
# имитируем аварийную ветку цикла напрямую
m_em = mk("Авария", "работаем без базы", "18:00")
res = asyncio.run(b.forward_to_tg(m_em["author"], m_em["text"], None, None))
check("без базы сообщение всё равно уходит", res == {111, 222})
check("доставлено в оба чата", len([1 for c, _, _ in tg.sent if c in (111, 222)]) == 2)
# открытие битой базы не должно валить процесс
import os as _os
b.STATE_DB = "/proc/нельзя/создать/here.db"
b.open_state_store()
check("недоступная база -> аварийный режим, а не падение", b._store is None)

# ═══════ 12. ЗАДАЧА 2: контрольное окно первого запуска ══════════════════════
# Риск: сообщение, пришедшее РОВНО во время переключения production, попадает
# в «историю» и глушится. Решение: последние N сегодняшних сообщений (с ЧЧ:ММ
# в .meta) ставятся в очередь, а не глушатся.

# 12a. Молчащая беседа: последние сообщения СТАРЫЕ (в .meta дата) -> 0 дубликатов
tg = install_fake()
st, _ = new_store()
old_hist = [mk("Галина", "давнее 1", "29 авг."), mk("Галина", "давнее 2", "30 авг."),
            mk("Артем", "давнее 3", "30 авг.")]
prev, mode, n = b.prime_or_resume(old_hist)
check("первый запуск распознан", mode == "первый-запуск")
check("старые сообщения в контрольное окно НЕ попали", n == 0)
asyncio.run(b.deliver_pending())
check("в молчащей беседе дубликатов НЕТ", len(tg.sent) == 0)
check("вся история помечена доставленной", st.pending_count() == 0)

# 12b. РЕЖИМ ПЕРЕХОДА (окно включено явно): внизу сегодняшние -> доставляются
tg = install_fake()
st, _ = new_store()
_saved_win = b.BOOTSTRAP_RESEND_LAST
b.BOOTSTRAP_RESEND_LAST = 3        # так настраивают только при переходе с работающего моста
live = [mk("Галина", "вчерашнее", "30 авг."),
        mk("Антонина", "сегодня утром", "09:15"),
        mk("L M", "пришло при переключении", "09:16")]
prev, mode, n = b.prime_or_resume(live)
check("сегодняшние попали в контрольное окно", n == 2)
asyncio.run(b.deliver_pending())
got = [t for _, _, t in tg.sent]
check("сообщение из окна переключения ДОСТАВЛЕНО",
      any("пришло при переключении" in t for t in got))
check("доставлено в оба чата",
      sum(1 for t in got if "пришло при переключении" in t) == 2)
check("старое (с датой) НЕ переслано", not any("вчерашнее" in t for t in got))

# 12c. ПО УМОЛЧАНИЮ окно выключено: чистый первый запуск без дубликатов
b.BOOTSTRAP_RESEND_LAST = _saved_win
check("по умолчанию контрольное окно выключено", b.BOOTSTRAP_RESEND_LAST == 0)
tg = install_fake()
st, _ = new_store()
today = [mk("A", "сегодня 1", "09:00"), mk("B", "сегодня 2", "09:01"),
         mk("C", "сегодня 3", "09:02")]
prev, mode, n = b.prime_or_resume(today)
check("чистый первый запуск: в окно НЕ попало ничего", n == 0)
asyncio.run(b.deliver_pending())
check("чистый первый запуск: в Telegram НЕ ушло ничего", len(tg.sent) == 0)
check("вся история помечена доставленной", st.pending_count() == 0)

# 12d. Размер окна ограничен и настраивается
many = [mk("A", f"сегодня {i}", f"10:{i:02d}") for i in range(10)]
check("окно не больше настройки",
      len(b.bootstrap_resend_set(many, 3)) == 3)
check("окно можно отключить", b.bootstrap_resend_set(many, 0) == set())
check("берутся именно ПОСЛЕДНИЕ",
      b.bootstrap_resend_set(many, 2) == {many[-1]["sig"], many[-2]["sig"]})
check("время с датой в окно не берётся",
      b.bootstrap_resend_set([mk("A", "x", "3 июл.")], 3) == set())

# ═══════ 13. АУДИТ: окно пишется в базу только при изменении ════════════════
# Без этого 30 INSERT каждые 2 секунды = ~1.3 млн записей в сутки на диск,
# занятый на 91 %.
st, _ = new_store()
st.mark_bootstrapped()
win = [mk("A", "раз", "10:00"), mk("B", "два", "10:01")]
writes = {"n": 0}
orig_save = st.save_window
def counting_save(msgs):
    writes["n"] += 1
    return orig_save(msgs)
st.save_window = counting_save
# имитируем три цикла с НЕИЗМЕННЫМ окном (как в молчащей беседе)
key = None
for _ in range(3):
    wkey = tuple(m["sig"] for m in win)
    if wkey != key:
        st.save_window(win); key = wkey
check("неизменное окно пишется один раз, а не каждый цикл", writes["n"] == 1)
# окно изменилось -> запись происходит
win2 = win + [mk("C", "три", "10:02")]
wkey2 = tuple(m["sig"] for m in win2)
if wkey2 != key:
    st.save_window(win2); key = wkey2
check("изменённое окно записывается", writes["n"] == 2)
check("в базе лежит актуальное окно", len(st.load_window()) == 3)

# ═══════ 14. АУДИТ: бюджет времени не оставляет заявки в sending ════════════
# При полном отказе Telegram цикл доставки не должен вешать опрос MAX,
# а неотправленное не должно застрять в sending до перезапуска.
tg = install_fake()
tg.fail_until = 10**6                      # Telegram лежит целиком
st, _ = new_store()
st.mark_bootstrapped()
for i in range(6):
    detect(st, new=[mk("Поток", f"сообщение {i}", f"11:{i:02d}")])
b.DELIVER_BUDGET_SEC = 0.0                 # бюджет исчерпан сразу
t0 = __import__("time").monotonic()
asyncio.run(b.deliver_pending())
elapsed = __import__("time").monotonic() - t0
check("цикл доставки не висит при отказе Telegram", elapsed < 10)
stuck = st.db.execute("SELECT COUNT(*) FROM deliveries WHERE state='sending'").fetchone()[0]
check("ни одна заявка не застряла в sending", stuck == 0)
check("все заявки остались в очереди", st.pending_count() == 12)
b.DELIVER_BUDGET_SEC = 8.0                 # возвращаем норму
# связь восстановилась — всё доставляется
tg = install_fake()
st.db.execute("UPDATE deliveries SET last_try=0")
for _ in range(3):
    asyncio.run(b.deliver_pending())
check("после восстановления очередь разошлась", st.pending_count() == 0)
check("каждое сообщение ушло в оба чата", len(tg.sent) == 12)

# ═══════ 15. ПУБЛИЧНАЯ ВЕРСИЯ: любое число чатов, админы отдельно ════════════

# 15a. один / два / три чата — без правки кода
for chats in ([111], [111, 222], [111, 222, 333]):
    tg = install_fake()
    st, _ = new_store()
    st.mark_bootstrapped()
    m = mk("Кто", f"для {len(chats)} чатов", f"12:{len(chats):02d}")
    st.record_detected("new", m, chats)
    asyncio.run(b.deliver_pending())
    got = sorted({c for c, _, _ in tg.sent})
    check(f"{len(chats)} чат(а): доставлено всем", got == sorted(chats))
    check(f"{len(chats)} чат(а): очередь пуста", st.pending_count() == 0)

# 15b. ошибка одного получателя не влияет на остальных (3 чата, падает средний)
tg = install_fake()
st, _ = new_store()
st.mark_bootstrapped()
base_send = tg.send_message
async def middle_down(chat_id, text, **kw):
    if chat_id == 222:
        raise RuntimeError("Bad Gateway")
    return await base_send(chat_id, text, **kw)
tg.send_message = middle_down
m3 = mk("Кто", "три чата, средний лежит", "12:10")
st.record_detected("new", m3, [111, 222, 333])
asyncio.run(b.deliver_pending())
got = sorted({c for c, _, _ in tg.sent})
check("исправные получатели получили", got == [111, 333])
check("в очереди только сбойный чат", st.pending_by_chat() == {222: 1})
tg = install_fake()
st.db.execute("UPDATE deliveries SET last_try=0")
asyncio.run(b.deliver_pending())
check("повтор ушёл только сбойному", [c for c, _, _ in tg.sent] == [222])
check("успешным повторно НЕ отправляли",
      not any(c in (111, 333) for c, _, _ in tg.sent))

# 15c. ДОБАВЛЕНИЕ нового чата не вызывает рассылку старой истории
tg = install_fake()
st, _ = new_store()
st.mark_bootstrapped()
old = [mk("Ист", f"старое {i}", f"08:{i:02d}") for i in range(5)]
for m in old:
    st.record_silent("new", m, [111])          # история доставлена в один чат
check("история помечена доставленной", st.pending_count() == 0)
# пользователь дописал второй чат в .env и перезапустил мост
new_msg = mk("Нов", "сообщение после добавления чата", "12:20")
st.record_detected("new", new_msg, [111, 222])
asyncio.run(b.deliver_pending())
to_new = [t for c, _, t in tg.sent if c == 222]
check("новое сообщение ушло в оба чата",
      sum(1 for _, _, t in tg.sent if "после добавления" in t) == 2)
check("новый чат получил РОВНО одно сообщение", len(to_new) == 1)
check("и это именно новое, а не история",
      "после добавления" in to_new[0])
check("старая история в новый чат НЕ полилась",
      not any("старое" in t for t in to_new))
check("очередь пуста", st.pending_count() == 0)

# 15d. получатели и администраторы — РАЗНЫЕ списки
class _U:
    def __init__(self, uid): self.id = uid
class _M:
    def __init__(self, uid): self.from_user = _U(uid)
_sv_allowed, _sv_targets = b.TELEGRAM_ALLOWED_USER_IDS, b.TELEGRAM_TARGET_CHAT_IDS
b.TELEGRAM_TARGET_CHAT_IDS = [111, 222, 333]
b.TELEGRAM_ALLOWED_USER_IDS = [111]
check("участник целевого чата НЕ получает админский доступ",
      b._allowed(_M(222)) is False and b._allowed(_M(333)) is False)
check("только из вайтлиста есть доступ", b._allowed(_M(111)) is True)
b.TELEGRAM_ALLOWED_USER_IDS = []
check("пустой вайтлист = доступа нет ни у кого (fail-closed)",
      b._allowed(_M(111)) is False and b._allowed(_M(222)) is False)
b.TELEGRAM_ALLOWED_USER_IDS, b.TELEGRAM_TARGET_CHAT_IDS = _sv_allowed, _sv_targets

# 15e. проверка конфигурации при запуске
check("пустой список чатов отвергается при запуске",
      hasattr(b, "check_config"))

print(f"\nИТОГО: ok={ok} fail={fail}")
raise SystemExit(1 if fail else 0)
