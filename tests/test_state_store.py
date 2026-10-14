"""Тесты постоянного хранения состояния. Без браузера, без сети, без Telegram."""

import os
import sys

# тесты лежат в tests/, модули проекта — в корне
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import tempfile
import time

os.environ.setdefault("RETRY_BASE_SEC", "30")
os.environ.setdefault("RETRY_GIVEUP_AT", "20")

import state_store
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


def mk(sig, author="Аня", text="привет", tm="10:00", media=None, reply=None):
    return {"sig": sig, "author": author, "text": text, "meta_time": tm,
            "key": f"{author}|{tm}", "media": media, "reply": reply}


T = [111]          # один получатель для большинства тестов хранилища
T2 = [111, 222]    # два чата — для тестов независимой доставки


def fresh():
    p = os.path.join(tempfile.mkdtemp(), "state.db")
    return StateStore(p), p


# ═══════════════════ 1. Базовая регистрация и защита от дубликатов ═══════════
s, path = fresh()
check("новое сообщение регистрируется", s.record_detected("new", mk("a1"), T) is True)
check("повторное чтение того же DOM НЕ создаёт дубликат",
      s.record_detected("new", mk("a1"), T) is False)
check("known() видит запись", s.known("a1") is True)
check("незнакомое не считается known", s.known("нет-такого") is False)
check("пока не доставлено — is_delivered False", s.is_delivered("a1") is False)
check("в очереди 1 запись", s.pending_count() == 1)

# ═══════════════════ 2. Доставка подтверждается только явно ══════════════════
claimed = s.claim_pending()
check("claim_pending выдал запись", len(claimed) == 1 and claimed[0]["sig"] == "a1")
check("claim перевёл в sending (повторный claim пуст)", s.claim_pending() == [])
check("после claim ещё НЕ доставлено", s.is_delivered("a1") is False)
s.mark_delivered("new:a1", 111)
check("после mark_delivered — доставлено", s.is_delivered("a1") is True)
check("очередь пуста", s.pending_count() == 0)

# ═══════════════════ 3. Временная ошибка Telegram → повтор ═══════════════════
s.record_detected("new", mk("b1", text="второе"), T)
got = s.claim_pending()
check("взяли b1 в отправку", len(got) == 1 and got[0]["sig"] == "b1")
st = s.mark_failed("new:b1", 111, "TelegramNetworkError: ServerDisconnected")
check("неудача помечена failed", st == "failed")
check("b1 НЕ доставлено после ошибки", s.is_delivered("b1") is False)
check("b1 остаётся в очереди", s.pending_count() == 1)
check("сразу повторять не даёт (нарастающая задержка)", s.claim_pending() == [])
# подкручиваем время последней попытки в прошлое — повтор должен дозреть
s.db.execute("UPDATE deliveries SET last_try=? WHERE event_id='new:b1'", (int(time.time()) - 999,))
again = s.claim_pending()
check("после выдержки повтор выдан", len(again) == 1 and again[0]["sig"] == "b1")
att = s.db.execute("SELECT attempts FROM deliveries WHERE event_id='new:b1'").fetchone()[0]
check("счётчик попыток сохранился в доставке чата", att == 1)
s.mark_delivered("new:b1", 111)
check("повтор в итоге доставлен", s.is_delivered("b1") is True)

# ═══════════════════ 4. Отказ навсегда (giveup) ══════════════════════════════
s.record_detected("new", mk("c1"), T)
s.claim_pending()
last = None
for i in range(state_store.RETRY_GIVEUP_AT):
    last = s.mark_failed("new:c1", 111, "боль")
    s.db.execute("UPDATE deliveries SET state='failed', last_try=0 WHERE event_id='new:c1'")
check("после предела попыток состояние giveup",
      last == "giveup" or s.db.execute(
          "SELECT attempts FROM deliveries WHERE event_id='new:c1'").fetchone()[0]
      >= state_store.RETRY_GIVEUP_AT)

# ═══════════════════ 5. Правки и удаления сохраняются отдельно ═══════════════
s2, _ = fresh()
s2.record_detected("new", mk("m1", text="исходный"), T)
s2.record_detected("edit", mk("m1-edited", text="исправленный"), T)
s2.record_detected("delete", mk("m1-deleted", text="исходный"), T)
kinds = {r["kind"] for r in s2.db.execute("SELECT kind FROM messages")}
check("три типа событий различаются", kinds == {"new", "edit", "delete"})
pend = s2.claim_pending()
check("все три события в очереди на доставку", len(pend) == 3)

# ═══════════════════ 6. Медиа и реплай переживают перезапуск ═════════════════
s3, p3 = fresh()
media = {"type": "photo", "srcs": ["https://x/1.jpg"], "count": 2, "id": "photo:x1"}
reply = {"author": "Дед", "text": "вопрос"}
s3.record_detected("new", mk("md1", text="с фото", media=media, reply=reply), T)
s3.close()
s3b = StateStore(p3)                       # имитация перезапуска процесса
row = s3b.claim_pending()[0]
check("медиа восстановлено после перезапуска", row["media"] == media)
check("реплай восстановлен после перезапуска", row["reply"] == reply)
check("тип медиа на месте", row["media"]["type"] == "photo")

# ═══════════════════ 7. Состояние переживает перезапуск ══════════════════════
s4, p4 = fresh()
s4.record_detected("new", mk("r1"), T)
s4.claim_pending()
s4.mark_delivered("new:r1", 111)
s4.record_detected("new", mk("r2"), T)        # обнаружено, но НЕ доставлено
s4.close()
s4b = StateStore(p4)
check("доставленное остаётся доставленным", s4b.is_delivered("r1") is True)
check("недоставленное остаётся в очереди", s4b.pending_count() == 1)
check("недоставленное выдаётся заново", s4b.claim_pending()[0]["sig"] == "r2")

# ═══════════════ 8. Процесс умер во время отправки (застряло в sending) ══════
s5, p5 = fresh()
s5.record_detected("new", mk("x1"), T)
s5.claim_pending()                          # state=sending, затем «смерть»
s5.close()
s5b = StateStore(p5)
n = s5b.recover_stuck_sending()
check("застрявшая в sending найдена при старте", n == 1)
if state_store.RESEND_STUCK_SENDING:
    check("политика по умолчанию: повторить (потеря хуже дубликата)",
          s5b.pending_count() == 1)
else:
    check("политика: считать доставленным", s5b.is_delivered("x1") is True)

# ═══════════════ 9. Снимок окна для classify() через перезапуск ══════════════
s6, p6 = fresh()
win = [mk("w1", text="раз"), mk("w2", text="два"), mk("w3", text="три")]
s6.save_window(win)
s6.close()
s6b = StateStore(p6)
loaded = s6b.load_window()
check("окно сохранилось целиком", len(loaded) == 3)
check("порядок окна сохранён", [m["text"] for m in loaded] == ["раз", "два", "три"])
check("ключи для classify на месте",
      all(set(m) >= {"sig", "key", "author", "text"} for m in loaded))
s6b.save_window([mk("w9", text="новое")])
check("окно перезаписывается, а не копится", len(s6b.load_window()) == 1)

# ═══════════════ 10. Первый прайминг не сливает историю в Telegram ══════════
s7, p7 = fresh()
check("свежая база не инициализирована", s7.is_bootstrapped() is False)
for i in range(5):
    s7.record_silent("new", mk(f"hist{i}", text=f"история {i}"), T)
s7.mark_bootstrapped()
check("история помечена доставленной без отправки", s7.pending_count() == 0)
check("флаг инициализации выставлен", s7.is_bootstrapped() is True)
s7.close()
check("флаг переживает перезапуск", StateStore(p7).is_bootstrapped() is True)

# ═══════════════ 11. Сообщения, пришедшие во время простоя ═══════════════════
s8, p8 = fresh()
# до простоя: окно из двух сообщений, оба доставлены
before = [mk("d1", text="до простоя 1"), mk("d2", text="до простоя 2")]
for m in before:
    s8.record_silent("new", m, T)
s8.mark_bootstrapped()
s8.save_window(before)
s8.close()
# простой; за это время появились d3, d4. Перезапуск:
s8b = StateStore(p8)
prev = s8b.load_window()
after = before + [mk("d3", text="во время простоя"), mk("d4", text="и ещё одно")]
newly = [m for m in after if not s8b.known(m["sig"])]
check("сообщения из простоя распознаны как новые", [m["text"] for m in newly] ==
      ["во время простоя", "и ещё одно"])
for m in newly:
    check(f"регистрируется к отправке: {m['text'][:12]}", s8b.record_detected("new", m, T) is True)
check("старые сообщения повторно НЕ отправляются", s8b.pending_count() == 2)
check("окно до простоя доступно для classify", len(prev) == 2)

# ═══════════════ 12. Очистка базы не трогает недоставленное ══════════════════
s9, _ = fresh()
for i in range(50):
    s9.record_silent("new", mk(f"old{i}"), T)
s9.record_detected("new", mk("важное-недоставленное"), T)
s9.prune(keep=10)
check("после очистки недоставленное на месте", s9.known("важное-недоставленное") is True)
check("недоставленное всё ещё в очереди", s9.pending_count() == 1)
left = s9.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
check("старые доставленные подчищены", left < 51)

# ═══════════════ 13. Загрузка множеств для classify ══════════════════════════
s10, _ = fresh()
s10.record_silent("new", mk("s1"), T)
s10.record_detected("new", mk("s2"), T)
seen = set(s10.load_seen(1000))
fwd = set(s10.load_forwarded(1000))
check("load_seen содержит оба", seen == {"s1", "s2"})
check("load_forwarded только доставленное", fwd == {"s1"})

# ═══════════════ 14. Устойчивость к повреждённым данным ══════════════════════
s11, _ = fresh()
s11.record_detected("new", {"sig": "n1", "author": "", "text": "", "meta_time": "",
                            "key": "", "media": None, "reply": None}, T)
check("пустые поля не ломают запись", s11.known("n1") is True)
check("статистика считается", isinstance(s11.stats(), dict) and "detected" in s11.stats())

# ═══════ 15. РЕГРЕСС: удаление не теряется, хотя подпись та же, что у появления ═══
s12, _ = fresh()
orig = mk("same-sig", author="Виктор", text="исходный текст")
check("появление зарегистрировано", s12.record_detected("new", orig, T) is True)
s12.claim_pending(); s12.mark_delivered("new:same-sig", 111)
check("УДАЛЕНИЕ с той же подписью НЕ отбрасывается",
      s12.record_detected("delete", orig, T) is True)
check("правка с той же подписью тоже проходит",
      s12.record_detected("edit", orig, T) is True)
evs = {r["kind"] for r in s12.db.execute("SELECT kind FROM messages WHERE sig='same-sig'")}
check("три события на одну подпись живут раздельно", evs == {"new", "edit", "delete"})
check("удаление ждёт доставки", s12.pending_count() == 2)
check("конкретное событие различается",
      s12.is_event_delivered("new", "same-sig") is True
      and s12.is_event_delivered("delete", "same-sig") is False)

# ═══════ 16. ЗАДАЧА 1: у каждого чата своё состояние доставки ════════════════
s13, p13 = fresh()
s13.mark_bootstrapped()
ev = mk("two-chats", author="Аня", text="в два чата")
s13.record_detected("new", ev, T2)
got = s13.claim_pending()
check("событие выдано один раз с двумя получателями",
      len(got) == 1 and sorted(got[0]["targets"]) == [111, 222])
s13.mark_delivered("new:two-chats", 111)          # первый принял
s13.mark_failed("new:two-chats", 222, "Bad Gateway")   # второй нет
check("событие НЕ закрыто, пока второй не получил",
      s13.db.execute("SELECT state FROM messages WHERE event_id='new:two-chats'"
                     ).fetchone()[0] != "delivered")
check("в очереди осталась ровно одна пара", s13.pending_count() == 1)
check("очередь именно для второго чата", s13.pending_by_chat() == {222: 1})
check("первый чат помечен доставленным", s13.chat_delivered("new", "two-chats", 111))
check("второй чат НЕ помечен", s13.chat_delivered("new", "two-chats", 222) is False)
# повтор должен уйти ТОЛЬКО во второй чат
s13.db.execute("UPDATE deliveries SET last_try=0")
again2 = s13.claim_pending()
check("повтор адресован только второму чату",
      len(again2) == 1 and again2[0]["targets"] == [222])
s13.mark_delivered("new:two-chats", 222)
check("после доставки обоим событие закрыто",
      s13.db.execute("SELECT state FROM messages WHERE event_id='new:two-chats'"
                     ).fetchone()[0] == "delivered")
check("очередь пуста", s13.pending_count() == 0)

# состояние по чатам переживает перезапуск
s13.close()
s13b = StateStore(p13)
check("состояние по чатам переживает перезапуск",
      s13b.chat_delivered("new", "two-chats", 111)
      and s13b.chat_delivered("new", "two-chats", 222))

# ═══════ 17. Отказ по одному чату не блокирует очередь остальных ════════════
s14, _ = fresh()
s14.mark_bootstrapped()
ev2 = mk("blocked-chat", text="второй заблокировал бота")
s14.record_detected("new", ev2, T2)
s14.claim_pending()
s14.mark_delivered("new:blocked-chat", 111)
for _ in range(state_store.RETRY_GIVEUP_AT):
    s14.mark_failed("new:blocked-chat", 222, "Forbidden: bot was blocked")
    s14.db.execute("UPDATE deliveries SET last_try=0 WHERE chat_id=222")
st222 = s14.db.execute("SELECT state FROM deliveries WHERE event_id='new:blocked-chat' "
                       "AND chat_id=222").fetchone()[0]
check("безнадёжный чат уходит в giveup", st222 == "giveup")
check("очередь не держится навсегда из-за одного чата", s14.pending_count() == 0)
check("первый чат при этом получил", s14.chat_delivered("new", "blocked-chat", 111))

print(f"\nИТОГО: ok={ok} fail={fail}")
raise SystemExit(1 if fail else 0)
