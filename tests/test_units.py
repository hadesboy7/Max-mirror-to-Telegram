"""Юнит-тесты чистых функций моста (без браузера и сети)."""

import os
import sys

# тесты лежат в tests/, модули проекта — в корне
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os

os.environ["TELEGRAM_BOT_TOKEN"] = "123:TEST"
os.environ["TELEGRAM_TARGET_CHAT_IDS"] = "111, 222,,xx,333"
os.environ["TELEGRAM_ALLOWED_USER_IDS"] = "111,222"
os.environ["SIGN_MAP"] = "111=Валера, 222=Аня, bad=x"
os.environ["SEEN_MAX"] = "5"
os.environ["MIRROR_OUTGOING"] = "0"   # фиксируем независимо от .env

import bridge_ui as b

ok = fail = 0


def check(name, cond):
    global ok, fail
    if cond:
        ok += 1
        print(f"  ok  {name}")
    else:
        fail += 1
        print(f"FAIL  {name}")


def mk(author, text, tm):
    return {"key": f"{author}|{tm}", "author": author, "text": text,
            "sig": b._sig(author, text, tm)}


# --- парсинг конфига ---
check("_ids корректные", b.TELEGRAM_TARGET_CHAT_IDS == [111, 222, 333])
check("_ids вайтлист", b.TELEGRAM_ALLOWED_USER_IDS == [111, 222])
check("SIGN_MAP пары", b.SIGN_MAP == {111: "Валера", 222: "Аня"})

# --- _sig ---
check("_sig различает время", b._sig("Аня", "ок", "09:13") != b._sig("Аня", "ок", "09:14"))
check("_sig стабилен", b._sig("Аня", "ок", "09:13") == b._sig("Аня", "ок", "09:13"))

# --- ограничение памяти ---
for i in range(20):
    b._seen_add(f"k{i}")
check("_seen ограничен SEEN_MAX", len(b._seen) <= 5)
check("_seen хранит последние", "k19" in b._seen and "k0" not in b._seen)

# --- _extract_incoming ---
msgs = [
    {"author": "Аня", "text": "привет", "time": "10:00", "variant": "incoming"},
    {"author": "Я", "text": "моё", "time": "10:01", "variant": "outgoing"},
    {"author": "Аня", "text": "", "time": "10:02", "variant": "incoming"},
    {"author": "Дед", "text": "ок", "time": "10:03", "variant": "incoming"},
    {"author": "Баба", "text": "ок", "time": "10:04", "variant": "incoming"},
]
inc = b._extract_incoming(msgs)
check("_extract_incoming фильтрует outgoing/пустые", len(inc) == 3)
check("_extract_incoming структура dict",
      set(inc[0]) == {"key", "author", "text", "meta_time", "media", "reply", "sig"})
check("_extract_incoming сохраняет строку времени для хранилища",
      inc[0]["meta_time"] == "10:00")
check("_extract_incoming повтор 'ок' не потерян", [m["text"] for m in inc].count("ок") == 2)
check("пустой автор -> 'Кто-то'",
      b._extract_incoming([{"author": "", "text": "hi", "time": "1", "variant": "incoming"}])[0]["author"] == "Кто-то")

# --- медиа: сообщение без текста, но с фото — не пропускается ---
mmsg = [{"author": "Аня", "text": "", "time": "5", "variant": "incoming",
         "media": {"type": "photo", "src": "https://x/y?tok=1", "id": "https://x/y"}}]
mi = b._extract_incoming(mmsg)
check("медиа без текста не пропущено", len(mi) == 1 and mi[0]["media"]["type"] == "photo")
check("медиа: sig по id (не по expiring src)",
      mi[0]["sig"] == b._sig("Аня", "https://x/y", "5"))

# --- наследование автора в сгруппированных сообщениях ---
grp = [
    {"author": "АНЮТКА", "text": "раз", "time": "1", "variant": "incoming"},
    {"author": "", "text": "два", "time": "2", "variant": "incoming"},
    {"author": "", "text": "", "time": "3", "variant": "incoming",
     "media": {"type": "photo", "id": "p", "count": 2, "pending": True}},
    {"author": "Другой", "text": "я новый", "time": "4", "variant": "incoming"},
]
gi = b._extract_incoming(grp)
check("группировка: 2-е сообщение наследует автора", gi[1]["author"] == "АНЮТКА")
check("группировка: медиа наследует автора", gi[2]["author"] == "АНЮТКА")
check("группировка: новый автор сбрасывает наследование", gi[3]["author"] == "Другой")

# начало серии без имени (есть аватар) -> НЕ наследуем чужого, ставим "Кто-то"
grp2 = [
    {"author": "Артем", "text": "a", "time": "1", "variant": "incoming"},
    {"author": "", "text": "б", "time": "2", "variant": "incoming", "hasAvatar": True},
]
gi2 = b._extract_incoming(grp2)
check("группировка: начало серии без имени -> Кто-то (не Артем)", gi2[1]["author"] == "Кто-то")

# карта аватар->имя: безымянное сообщение с известным аватаром -> подставить имя
grp3 = [
    {"author": "АНЮТКА", "text": "a", "time": "1", "variant": "incoming", "avatar": "https://av/anyutka1"},
    {"author": "", "text": "б", "time": "2", "variant": "incoming", "hasAvatar": True, "avatar": "https://av/anyutka1"},
]
gi3 = b._extract_incoming(grp3)
check("карта аватар->имя: безымянное с известным аватаром -> имя", gi3[1]["author"] == "АНЮТКА")

# --- цитаты (реплаи) ---
check("_quote пусто без реплая", b._quote(None) == "" and b._quote({}) == "")
_q = b._quote({"author": "Артем", "text": "Разметка и шлагбаум"})
check("_quote есть автор+текст", "Артем" in _q and "Разметка" in _q and "blockquote" in _q)
check("_quote экранирует html", "&lt;" in b._quote({"author": "<b>x", "text": "y"}))
_h = b._head("Аня", "мой ответ", {"author": "Дед", "text": "исходное"})
check("_head со ссылкой-цитатой: цитата ПЕРЕД автором",
      _h.index("Дед") < _h.index("Аня") and "мой ответ" in _h)
_ri = b._extract_incoming([{"author": "Аня", "text": "ответ", "time": "1",
                            "variant": "incoming",
                            "reply": {"author": "Дед", "text": "вопрос"}}])
check("_extract_incoming проносит reply", _ri[0]["reply"]["author"] == "Дед")

# ======================= classify =======================
# 1) без изменений
p = [mk("A", "one", "1"), mk("B", "two", "2")]
n, e, d = b.classify(p, list(p), set(), set())
check("classify: без изменений — пусто", (n, e, d) == ([], [], []))

# 2) новое сообщение снизу
cur = p + [mk("C", "three", "3")]
n, e, d = b.classify(p, cur, seen=set(), forwarded=set())
check("classify: новое сообщение", len(n) == 1 and n[0]["text"] == "three" and not e and not d)

# 3) правка (тот же слот, другой текст), оригинал был отправлен -> edited
orig = mk("B", "two", "2")
edited_cell = mk("B", "two-edited", "2")
n, e, d = b.classify([mk("A", "one", "1"), orig],
                     [mk("A", "one", "1"), edited_cell],
                     seen=set(), forwarded={orig["sig"]})
check("classify: правка -> edited", len(e) == 1 and e[0]["text"] == "two-edited" and not n and not d)

# 4) правка, но оригинал НЕ отправляли -> трактуем как новое (если не seen)
n, e, d = b.classify([mk("A", "one", "1"), orig],
                     [mk("A", "one", "1"), edited_cell],
                     seen=set(), forwarded=set())
check("classify: правка непоказанного -> new", len(n) == 1 and not e)

# 5) удаление из середины (оригинал был отправлен) -> deleted
pa, pb, pc = mk("A", "one", "1"), mk("B", "two", "2"), mk("C", "three", "3")
n, e, d = b.classify([pa, pb, pc], [pa, pc], seen=set(), forwarded={pb["sig"]})
check("classify: удаление из середины -> deleted", len(d) == 1 and d[0]["text"] == "two" and not n)

# 6) исчезновение сверху (прокрутка) -> НЕ удаление
n, e, d = b.classify([pa, pb, pc], [pb, pc], seen=set(), forwarded={pa["sig"]})
check("classify: верхняя прокрутка не считается удалением", d == [])

# 7) прокрутка + новое снизу
n, e, d = b.classify([pa, pb, pc], [pb, pc, mk("D", "four", "4")],
                     seen=set(), forwarded={pa["sig"]})
check("classify: прокрутка+новое", len(n) == 1 and n[0]["text"] == "four" and d == [])

# 8) удаление, но оригинал НЕ отправляли (история) -> молчим
n, e, d = b.classify([pa, pb, pc], [pa, pc], seen=set(), forwarded=set())
check("classify: удаление непоказанного — молчим", d == [])

# 9) НИЖНИЙ дребезг: исчезновение у нижнего края НЕ считается удалением (C2)
n, e, d = b.classify([pa, pb, pc], [pa, pb], seen=set(), forwarded={pc["sig"]})
check("classify: нижний дребезг не считается удалением", d == [] and n == [])

# 10) удаление предпоследнего (внутри окна) при живом последнем -> deleted
n, e, d = b.classify([pa, pb, pc], [pa, pc], seen=set(), forwarded={pb["sig"]})
check("classify: удаление предпоследнего (внутри) -> deleted",
      len(d) == 1 and d[0]["text"] == "two")

# --- дизамбигуация дублей (C3): два одинаковых сообщения не схлопываются в один sig ---
dups = b._extract_incoming([
    {"author": "Аня", "text": "ок", "time": "10:00", "variant": "incoming"},
    {"author": "Аня", "text": "ок", "time": "10:00", "variant": "incoming"},
])
check("дубли: разные sig у двух одинаковых сообщений", dups[0]["sig"] != dups[1]["sig"])
check("дубли: первый sig стабилен (без суффикса)",
      dups[0]["sig"] == b._sig("Аня", "ок", "10:00"))

# --- fail-closed вайтлист (S3) ---
class _U:
    def __init__(self, uid): self.id = uid
class _M:
    def __init__(self, uid): self.from_user = _U(uid)
_saved = b.TELEGRAM_ALLOWED_USER_IDS
b.TELEGRAM_ALLOWED_USER_IDS = []
check("fail-closed: пустой вайтлист -> никого не пускаем", b._allowed(_M(111)) is False)
b.TELEGRAM_ALLOWED_USER_IDS = [111]
check("fail-closed: свой в вайтлисте -> пускаем", b._allowed(_M(111)) is True)
check("fail-closed: чужой -> не пускаем", b._allowed(_M(999)) is False)
b.TELEGRAM_ALLOWED_USER_IDS = _saved

# --- экранирование в HTML (S4): автор/текст не пролезают тегами ---
check("_head экранирует автора", "&lt;b&gt;" in b._head("<b>", "", None))
check("_head экранирует текст", "&lt;i&gt;" in b._head("Аня", "<i>hi", None))

# --- caption для медиа режется под лимит Telegram (R4) ---
_long = "я" * 5000
_cap = b._head("Аня", _long, None, limit=b.MEDIA_CAPTION_TEXT)
check("медиа-подпись не длиннее лимита", len(_cap) <= b.TG_CAPTION_LIMIT)

# --- политика тишины: одиночное падение лечится молча, повторные -> сообщение ---
import tempfile as _tf
b.CRASH_FILE = _tf.mktemp()
_counts = [b._note_crash() for _ in range(4)]
check("счётчик падений растёт", _counts == [1, 2, 3, 4])
check("1-2 падения ниже порога (молчим)", b.CRASH_ALERT_AT > 2)
check("с 3-го падения порог достигнут (пишем)", _counts[2] >= b.CRASH_ALERT_AT)
try:
    os.remove(b.CRASH_FILE)
except OSError:
    pass

print(f"\nИТОГО: ok={ok} fail={fail}")
raise SystemExit(1 if fail else 0)
