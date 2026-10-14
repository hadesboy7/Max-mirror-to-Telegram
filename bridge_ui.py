"""
MAX (web.max.ru) <-> Telegram мост через UI-автоматизацию (без бота MAX).

Читает открытую беседу из веб-клиента MAX и льёт новые сообщения в Telegram.
Обратно (опция SEND_TO_MAX=1): пишешь Telegram-боту -> печатается в беседу.

Режимы:
  CALIBRATE=1  — headful, сдампить DOM в calib_dump.html (подобрать селекторы)
  TEST_READ=1  — один раз распечатать разобранные сообщения и выйти (без Telegram)
  (по умолчанию) — рабочий режим: зеркалит 24/7
"""

import asyncio
import hashlib
import html
import logging
import os
import re
import signal
import time
from collections import deque
from difflib import SequenceMatcher

from dotenv import load_dotenv

load_dotenv()  # читаем .env из текущей папки (надёжно с пробелами и спецсимволами)

from aiogram import Bot as TgBot, Dispatcher as TgDispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (Message as TgMessage, BufferedInputFile,
                           InputMediaPhoto, ReplyKeyboardMarkup, KeyboardButton)
from playwright.async_api import async_playwright

from state_store import StateStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger("mirror")

# ---------------------------------------------------------------------------
# Конфиг
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not TELEGRAM_BOT_TOKEN:
    raise SystemExit("Не задан TELEGRAM_BOT_TOKEN (см. .env.example)")


def _ids(name: str) -> list[int]:
    """Разбирает список id из .env. Порядок сохраняется, дубликаты убираются."""
    out = []
    for x in os.environ.get(name, "").replace(" ", "").split(","):
        if not x:
            continue
        try:
            v = int(x)
        except ValueError:
            log.warning("Пропускаю некорректный id в %s: %r", name, x)
            continue
        if v in out:
            log.warning("%s: id %s указан несколько раз — беру один", name, v)
            continue
        out.append(v)
    return out


# Куда зеркалить (несколько id через запятую):
TELEGRAM_TARGET_CHAT_IDS = _ids("TELEGRAM_TARGET_CHAT_IDS")
# Кто может писать в беседу через бота (вайтлист; для остальных бот молчит):
TELEGRAM_ALLOWED_USER_IDS = _ids("TELEGRAM_ALLOWED_USER_IDS")

# Подпись при записи в беседу MAX по id отправителя: "111=Имя,222=Другое"
SIGN_MAP: dict[int, str] = {}
for _pair in os.environ.get("SIGN_MAP", "").split(","):
    if "=" in _pair:
        _k, _v = _pair.split("=", 1)
        try:
            SIGN_MAP[int(_k.strip())] = _v.strip()
        except ValueError:
            pass

# Помечать изменённые/удалённые сообщения (best-effort, см. README):
TRACK_EDITS = os.environ.get("TRACK_EDITS", "1") == "1"
TRACK_DELETES = os.environ.get("TRACK_DELETES", "1") == "1"
# Зеркалить и свои (исходящие) сообщения — нужно для теста в «Избранном»:
MIRROR_OUTGOING = os.environ.get("MIRROR_OUTGOING", "0") == "1"

# ЗАЩИТА: отправка из Telegram В БЕСЕДУ выключена по умолчанию.
SEND_TO_MAX = os.environ.get("SEND_TO_MAX", "0") == "1"
# Тест: при старте один раз переслать последнее входящее (проверить канал MAX->TG)
TEST_FORWARD_LAST = os.environ.get("TEST_FORWARD_LAST", "0") == "1"

MAX_URL = os.environ.get("MAX_URL", "https://web.max.ru/")
USER_DATA_DIR = os.environ.get("USER_DATA_DIR", "./max_profile")
HEADFUL = os.environ.get("HEADFUL", "1") == "1"
CALIBRATE = os.environ.get("CALIBRATE", "0") == "1"
TEST_READ = os.environ.get("TEST_READ", "0") == "1"
POLL_SEC = float(os.environ.get("POLL_SEC", "2"))
PRIME_SETTLE = float(os.environ.get("PRIME_SETTLE_SEC", "5"))  # дать списку отрисоваться
# как часто писать в лог "жив" — чтобы молчание беседы отличалось от зависшего моста
HEARTBEAT_SEC = float(os.environ.get("HEARTBEAT_SEC", "1800"))
SEEN_MAX = int(os.environ.get("SEEN_MAX", "8000"))
TG_TEXT_LIMIT = 4000
MAX_SEND_LEN = int(os.environ.get("MAX_SEND_LEN", "3000"))  # предел длины TG->MAX сообщения
TG_CAPTION_LIMIT = 1024          # лимит подписи к фото/видео в Telegram (не 4000!)
# Таймаут на любую операцию со страницей: подвисший рендерер не должен вешать мост.
PAGE_OP_TIMEOUT = float(os.environ.get("PAGE_OP_TIMEOUT", "30"))
AVATAR_MAX = int(os.environ.get("AVATAR_MAX", "500"))   # предел карты аватар->имя
# Бюджет памяти на один альбом фото (иначе 10×45МБ = OOM на 956МБ RAM):
ALBUM_MAX_BYTES = int(os.environ.get("ALBUM_MAX_BYTES", str(60 * 1024 * 1024)))
# Сколько опросов подряд "composer есть, но 0 сообщений" до тревоги (селекторы отвалились):
STALE_ALERT_LOOPS = int(os.environ.get("STALE_ALERT_LOOPS", "300"))  # ~10 мин при POLL_SEC=2
# Сколько секунд ждать до тревоги о слетевшей сессии (короткий обрыв лечится сам):
SESSION_GRACE = float(os.environ.get("SESSION_GRACE_SEC", "300"))    # 5 мин молчания
QR_REPEAT_SEC = float(os.environ.get("QR_REPEAT_SEC", "1800"))       # напоминание раз в 30 мин

# --- постоянное хранение состояния (SQLite) ---
# Без него любой перезапуск терял сообщения: прайминг помечал всё видимое
# «виденным» без пересылки. См. state_store.py.
STATE_DB = os.environ.get("STATE_DB", "./state.db")
STATE_KEEP = int(os.environ.get("STATE_KEEP", "20000"))   # сколько записей держать
DELIVER_BATCH = int(os.environ.get("DELIVER_BATCH", "10"))  # отправок за один цикл
# Бюджет времени на доставку в одном цикле. При полном отказе Telegram каждая
# отправка тратит до ~1.5 с на внутренние повторы; без бюджета 10 событий на
# 2 чата заняли бы ~30 с и затормозили обнаружение новых сообщений.
# Недоставленное возвращается в очередь без штрафа и уйдёт следующим циклом.
# Бюджет = один интервал опроса: шаг доставки не должен растягивать цикл.
DELIVER_BUDGET_SEC = float(os.environ.get("DELIVER_BUDGET_SEC", "2"))
# Внутренних попыток на одну отправку. Раньше было 3 с паузами 0.5+1.0 с, то
# есть 1.5 с на каждый неудачный чат. Теперь 2 попытки: одиночный сбой связи
# по-прежнему поглощается, а глубокие отказы уходят в очередь с задержкой
# повтора, где им и место.
TG_RETRY_ATTEMPTS = int(os.environ.get("TG_RETRY_ATTEMPTS", "2"))
# Если Telegram недоступен ЦЕЛИКОМ (ни одной успешной отправки), шаг доставки
# пропускается на столько циклов. Иначе цикл опроса тормозил бы по несколько
# секунд подряд, пока все заявки не получат первую попытку. Ничего не теряется:
# заявки лежат в очереди, и у них всё равно своя задержка повтора 30 с.
DELIVER_PAUSE_CYCLES = int(os.environ.get("DELIVER_PAUSE_CYCLES", "15"))
# Политика DELIVER_REQUIRE_ALL больше не нужна: у каждого чата своё состояние
# доставки в таблице deliveries. Чат, который уже получил сообщение, повторно
# не беспокоим; чат, который не получил, остаётся в очереди именно для себя.
# Чат, навсегда заблокировавший бота, уходит в giveup сам и очередь не держит.
# Часть медиа достаётся ТОЛЬКО из живого DOM (карточка файла — кликом, стикер —
# снимком элемента, недогруженный альбом — чтением последнего блока). Если
# доставка отложилась (Telegram лежал, процесс перезапускался), DOM уже другой,
# и обращаться к нему нельзя: можно прислать ЧУЖОЕ вложение. Старше этого
# порога — отдаём текст с honest-пометкой вместо вложения.
DOM_FRESH_SEC = int(os.environ.get("DOM_FRESH_SEC", "120"))
# КОНТРОЛЬНОЕ ОКНО ПЕРВОГО ЗАПУСКА (по умолчанию ВЫКЛЮЧЕНО).
#
# При первом запуске с пустой базой вся видимая история помечается
# доставленной, иначе она хлынет в Telegram. Но если вы ПЕРЕХОДИТЕ с уже
# работавшего моста, сообщение, пришедшее ровно в момент переключения
# (~40-60 с), попадёт в эту «историю» и потеряется.
#
# Это окно ставит последние N сообщений в очередь на доставку вместо глушения.
# ЧЕСТНОЕ ОГРАНИЧЕНИЕ: отличить «пришло во время переключения» от «пришло
# сегодня утром» нельзя — у сообщений MAX нет ни идентификатора, ни полной
# даты, в .meta только ЧЧ:ММ (сегодня) либо дата (старое). Поэтому окно
# отбирает лишь СЕГОДНЯШНИЕ сообщения, и если беседа была активна раньше в
# этот же день, несколько из них придут повторно.
#
#   0 (по умолчанию) — чистый первый запуск, дубликатов нет. Подходит всем,
#                      кто ставит мост с нуля.
#   1-3              — только для перехода с уже работающего моста: закрывает
#                      окно потери ценой не более N разовых дубликатов.
BOOTSTRAP_RESEND_LAST = int(os.environ.get("BOOTSTRAP_RESEND_LAST", "0"))

# Селекторы веб-клиента MAX (откалибровано под web.max.ru, Svelte-версия).
SEL_MSG_ITEM = os.environ.get("SEL_MSG_ITEM") or ".bubbleContent"
SEL_MSG_AUTHOR = os.environ.get("SEL_MSG_AUTHOR") or ".header .name"
SEL_MSG_TEXT = os.environ.get("SEL_MSG_TEXT") or ":scope > span.text"
SEL_MSG_TIME = os.environ.get("SEL_MSG_TIME") or ".meta"
SEL_COMPOSER = os.environ.get("SEL_COMPOSER") or "[data-lexical-editor='true']"

# Доп. флаги Chromium (на сервере под root нужен --no-sandbox и экономия памяти):
#   CHROMIUM_ARGS="--no-sandbox --disable-dev-shm-usage --disable-gpu"
CHROMIUM_ARGS = os.environ.get("CHROMIUM_ARGS", "").split()

def check_config() -> None:
    """Проверяет конфигурацию при запуске и внятно сообщает об ошибках.

    Телеграм-чаты (куда пересылаем) и пользователи (кому разрешено управлять
    ботом) — ДВА РАЗНЫХ списка. Попадание чата в список получателей НЕ даёт
    административного доступа.
    """
    problems, notes = [], []

    if not TELEGRAM_TARGET_CHAT_IDS:
        problems.append(
            "TELEGRAM_TARGET_CHAT_IDS пуст — некуда пересылать сообщения. "
            "Укажите один или несколько id через запятую, например:\n"
            "    TELEGRAM_TARGET_CHAT_IDS=-1001111111111\n"
            "    TELEGRAM_TARGET_CHAT_IDS=-1001111111111,-1002222222222")
    if not MAX_URL or MAX_URL.rstrip("/") == "https://web.max.ru":
        problems.append(
            "MAX_URL не указывает на конкретную беседу. Откройте нужную беседу "
            "в web.max.ru и скопируйте адрес целиком, например:\n"
            "    MAX_URL=https://web.max.ru/-70000000000000")
    if not TELEGRAM_ALLOWED_USER_IDS:
        notes.append(
            "TELEGRAM_ALLOWED_USER_IDS пуст: команды бота (кнопка «Скриншот "
            "чата», /start) не будут работать НИ У КОГО. Это безопасное "
            "поведение по умолчанию. Чтобы разрешить себе — укажите свой "
            "числовой id (его сообщает @userinfobot).")
    if SEND_TO_MAX and not TELEGRAM_ALLOWED_USER_IDS:
        problems.append(
            "SEND_TO_MAX=1 вместе с пустым TELEGRAM_ALLOWED_USER_IDS — запись в "
            "беседу MAX была бы открыта без ограничений. Заполните вайтлист "
            "или выключите SEND_TO_MAX.")
    if POLL_SEC < 1:
        notes.append(f"POLL_SEC={POLL_SEC} — слишком частый опрос, "
                     "браузер будет нагружен без пользы.")

    for n in notes:
        log.warning("Внимание: %s", n)
    if problems:
        raise SystemExit("Ошибка конфигурации .env:\n\n" +
                         "\n\n".join(f"  - {p}" for p in problems))
    log.info("Конфигурация: целевых чатов %d, администраторов %d, "
             "запись в беседу %s",
             len(TELEGRAM_TARGET_CHAT_IDS), len(TELEGRAM_ALLOWED_USER_IDS),
             "ВКЛ" if SEND_TO_MAX else "выкл")


tg_bot = TgBot(TELEGRAM_BOT_TOKEN)
tg_dp = TgDispatcher()

_page = None
_ctx = None                      # persistent-контекст браузера (для чистого закрытия при выходе)
_page_lock = asyncio.Lock()      # сериализуем операции над страницей
_seen: set[str] = set()          # sig всех виденных сообщений (анти-повтор)
_seen_order: deque[str] = deque()
_forwarded: set[str] = set()     # sig сообщений, реально отправленных в TG
_forwarded_order: deque[str] = deque()
_prev_visible: list[dict] = []   # видимые сообщения прошлого опроса (для дифа)
_avatar_to_name: dict[str, str] = {}   # карта аватар-URL -> имя (для безымянных)
_primed = False
_store: StateStore | None = None       # постоянное хранилище состояния
_giveup_alerted = False                # чтобы не спамить про безнадёжные отправки
_last_window_key = None                # отпечаток последнего сохранённого окна
_deliver_skip = 0                      # сколько циклов пропустить доставку (Telegram лежит)


def _bounded_add(s: set, order: deque, key: str) -> None:
    if key in s:
        return
    s.add(key)
    order.append(key)
    while len(s) > SEEN_MAX:
        s.discard(order.popleft())


def _seen_add(key: str) -> None:
    _bounded_add(_seen, _seen_order, key)


def _forwarded_add(key: str) -> None:
    _bounded_add(_forwarded, _forwarded_order, key)


_avatar_order: deque[str] = deque()


def _avatar_remember(url: str, name: str) -> None:
    """Запоминает аватар->имя с ограничением размера (иначе рост без границ за месяцы)."""
    if url not in _avatar_to_name:
        _avatar_order.append(url)
        while len(_avatar_order) > AVATAR_MAX:
            _avatar_to_name.pop(_avatar_order.popleft(), None)
    _avatar_to_name[url] = name


def _sig(author: str, text: str, tm: str) -> str:
    return hashlib.sha1(f"{author}|{text}|{tm}".encode()).hexdigest()


def classify(prev: list[dict], cur: list[dict], seen: set, forwarded: set):
    """Диф видимого списка сообщений между опросами.

    prev/cur — списки {key, author, text, sig} в порядке DOM, где
      key  = author|время  (устойчив к правке текста)
      sig  = author|text|время (меняется при правке — для анти-повтора)
    Возвращает (new, edited, deleted) — списки message-словарей
    (для new/edited — из cur, для deleted — из prev).
    """
    new, edited, deleted = [], [], []
    sm = SequenceMatcher(a=[m["key"] for m in prev],
                         b=[m["key"] for m in cur], autojunk=False)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                p, c = prev[i1 + k], cur[j1 + k]
                if p["text"] != c["text"]:               # тот же слот, другой текст
                    if p["sig"] in forwarded:
                        edited.append(c)
                    elif c["sig"] not in seen:
                        new.append(c)
        elif tag in ("insert", "replace"):               # новые снизу / замена сверху
            for c in cur[j1:j2]:
                if c["sig"] not in seen:
                    new.append(c)
        elif tag == "delete":
            # Реальное удаление — только ВНУТРИ окна. Исчезновение у верхнего края
            # (i1==0) — прокрутка вверх; у нижнего (i2==len(prev)) — дребезг нижней
            # границы виртуального списка (scroll не докрутил) => НЕ считаем удалением,
            # иначе самые свежие сообщения ложно репортятся как 🗑, потом как новые.
            if i1 > 0 and i2 < len(prev):
                for p in prev[i1:i2]:
                    if p["sig"] in forwarded:
                        deleted.append(p)
    return new, edited, deleted


# ---------------------------------------------------------------------------
# Чтение сообщений из DOM
# ---------------------------------------------------------------------------
# JS: ищем контейнер ПО ФАКТУ прокрутки, а не по имени класса.
# Ловушка: .closest('[class*=scroll]') цепляет scrollListContent — внутреннюю
# обёртку, которая НЕ прокручивается (scrollHeight == clientHeight), и прокрут
# молча превращается в пустышку. Настоящий скроллер — ближайший предок,
# у которого есть переполнение и overflow-y: auto/scroll.
_JS_SCROLL_BOTTOM = """() => {
  const el = document.querySelector('.bubbleContent');
  if (!el) return {ok: false, why: 'нет сообщений'};
  let n = el.parentElement, sc = null;
  while (n && n !== document.body) {
    if (n.scrollHeight > n.clientHeight + 5) {
      const oy = getComputedStyle(n).overflowY;
      if (oy === 'auto' || oy === 'scroll' || oy === 'overlay') { sc = n; break; }
    }
    n = n.parentElement;
  }
  if (!sc) return {ok: false, why: 'скроллер не найден'};
  sc.scrollTop = sc.scrollHeight;
  return {ok: true, dist: Math.round(sc.scrollHeight - sc.scrollTop - sc.clientHeight)};
}"""


async def scroll_to_latest(passes: int = 3) -> bool:
    """Промотать ленту к самым новым сообщениям. True = докрутили до низа.

    КРИТИЧНО: MAX открывает беседу на позиции 'последнее прочитанное', а новые
    сообщения ниже, вне отрисованного окна виртуального списка. Без прокрутки
    вниз мост их не видит и не пересылает (баг 'застрял на старой дате').

    Несколько проходов: после прокрутки подгружается новая порция, и низ уезжает.
    """
    if not _page:
        return False
    res = {}
    try:
        for i in range(max(1, passes)):
            async with _page_lock:
                res = await asyncio.wait_for(
                    _page.evaluate(_JS_SCROLL_BOTTOM), PAGE_OP_TIMEOUT)
            if not res.get("ok"):
                break
            if res.get("dist", 1) <= 2:      # уже у самого низа — хватит
                return True
            await asyncio.sleep(0.4)         # дать догрузиться следующей порции
    except asyncio.TimeoutError:
        log.warning("scroll_to_latest: страница не ответила за %ss", PAGE_OP_TIMEOUT)
        raise
    except Exception:
        log.warning("scroll_to_latest: не смог промотать")
        return False
    if not res.get("ok"):
        log.warning("scroll_to_latest: %s — свежие сообщения могут не попасть в кадр",
                    res.get("why", "неизвестно"))
        return False
    return res.get("dist", 1) <= 2


async def read_messages() -> list[dict]:
    if not _page:
        return []
    js = """
    (sel) => {
      const items = Array.from(document.querySelectorAll(sel.item));
      return items.map((el) => {
        const a = sel.author ? el.querySelector(sel.author) : null;
        const t = sel.text ? el.querySelector(sel.text) : null;
        const tm = sel.time ? el.querySelector(sel.time) : null;
        const v = el.closest('[data-bubbles-variant]');
        const rawtime = (tm ? tm.innerText : '').trim();
        const tmatch = rawtime.match(/\\d{1,2}:\\d{2}/);   // только ЧЧ:ММ

        // --- медиа ---
        let media = null;
        const mediaBox = el.querySelector('.media, .attaches');
        const vsrc = mediaBox ? mediaBox.querySelector('video source') : null;
        const fileCard = el.querySelector('.fileIcon');
        const audioEl = el.querySelector('audio[src]');
        const stickerEl = el.querySelector('[data-testid^="sticker"]');
        // фото: img.image ВНУТРИ .media, но НЕ эмодзи (у эмодзи src .../emojis/...)
        let photoSrcs = [], expected = 0;
        if (mediaBox && !vsrc) {
          photoSrcs = [...mediaBox.querySelectorAll('img.image')]
              .map(i => i.src).filter(s => s && !s.includes('/emojis/'));
          const grid = mediaBox.querySelector('.grid');
          if (grid) expected = grid.querySelectorAll('.tile').length;
          else if (mediaBox.querySelector('img.image, .tile')) expected = Math.max(1, photoSrcs.length);
        }
        if (vsrc && vsrc.src) {
          const vid = mediaBox.querySelector('video');
          media = {type:'video', src: vsrc.src, id: vsrc.src.split('?')[0],
                   poster: vid ? (vid.getAttribute('poster')||'') : ''};
        } else if (expected > 0 || photoSrcs.length > 0) {
          media = {type:'photo', srcs: photoSrcs, count: Math.max(expected, photoSrcs.length),
                   pending: photoSrcs.length < expected,
                   id: 'photo:' + (photoSrcs[0] ? photoSrcs[0].split('?')[0] : ('n'+expected))};
        } else if (fileCard) {
          const ttl = fileCard.parentElement ? fileCard.parentElement.querySelector('.title') : null;
          const name = ttl ? ttl.innerText.trim() : 'файл';
          media = {type:'file', filename: name, id: 'file:'+name};
        } else if (audioEl) {
          media = {type:'audio', src: audioEl.src, id: 'audio:'+audioEl.src.split('?')[0]};
        } else if (stickerEl) {
          media = {type:'sticker', id: stickerEl.getAttribute('data-testid')||'sticker'};
        }
        // --- цитата (реплай): button.mark внутри пузыря ---
        // .mark .author .name -> автор оригинала; .mark > .text -> отрывок цитаты.
        // Обычное превью ссылки такого button.mark не содержит.
        let reply = null;
        const mark = el.querySelector('.mark');
        if (mark) {
          const qa = mark.querySelector('.author .name');
          let qt = '';
          for (const ch of mark.children) {            // прямой .text, не имя автора
            if (ch.classList && ch.classList.contains('text')) { qt = ch.innerText; break; }
          }
          reply = {author: (qa ? qa.innerText : '').trim(),
                   text: (qt || '').trim()};
        }

        const wrap = el.closest('.messageWrapper');
        const hasAvatar = !!(wrap && wrap.querySelector('.authorAvatar .avatar'));
        const avImg = wrap ? wrap.querySelector('.authorAvatar img') : null;
        return {
          variant: v ? v.getAttribute('data-bubbles-variant') : '',
          author: (a ? a.innerText : '').trim(),
          hasAvatar: hasAvatar,          // есть аватар => начало серии (не продолжение)
          avatar: avImg ? avImg.src : '',// картинка аватара (для карты аватар->имя)
          text: (t ? t.innerText : '').trim(),
          time: tmatch ? tmatch[0] : rawtime,
          media: media,
          reply: reply,                  // {author, text} цитируемого или null
        };
      });
    }
    """
    try:
        async with _page_lock:
            return await asyncio.wait_for(_page.evaluate(js, {
                "item": SEL_MSG_ITEM,
                "author": SEL_MSG_AUTHOR,
                "text": SEL_MSG_TEXT,
                "time": SEL_MSG_TIME,
            }), PAGE_OP_TIMEOUT)
    except asyncio.TimeoutError:
        log.error("read_messages: страница зависла (нет ответа за %ss)", PAGE_OP_TIMEOUT)
        raise                                   # пусть poll-цикл перезапустит процесс
    except Exception:
        log.exception("read_messages: ошибка чтения DOM")
        return []


def browser_alive() -> bool:
    """Жив ли браузер. Если он умер, страница закрыта и лечить нечего —
    процесс должен упасть, чтобы systemd поднял его с чистым браузером."""
    try:
        return _page is not None and not _page.is_closed()
    except Exception:
        return False


# Файл-пульс: мост трогает его каждый рабочий цикл. Сторож смотрит ТОЛЬКО на него —
# в отличие от журнала, это не ломается при переполнении диска или сбое journald.
def _runtime_path(name: str) -> str:
    """Куда класть служебные файлы состояния.

    Под root (служба systemd) — в /run, как и положено runtime-состоянию.
    Обычному пользователю /run недоступен, поэтому кладём рядом с проектом.
    Путь можно задать явно через .env.
    """
    for d in ("/run", "/var/run"):
        if os.path.isdir(d) and os.access(d, os.W_OK):
            return os.path.join(d, name)
    return os.path.join(".", name)


# Файл-пульс: мост трогает его каждый цикл, внешний сторож смотрит на дату.
ALIVE_FILE = os.environ.get("ALIVE_FILE") or _runtime_path("max-mirror.alive")


def _touch_alive() -> None:
    try:
        with open(ALIVE_FILE, "w") as f:
            f.write(str(int(time.time())))
    except Exception:
        pass


CRASH_FILE = os.environ.get("CRASH_FILE") or _runtime_path("max-mirror-crashes")
CRASH_WINDOW = float(os.environ.get("CRASH_WINDOW_SEC", "3600"))   # окно подсчёта падений
CRASH_ALERT_AT = int(os.environ.get("CRASH_ALERT_AT", "3"))        # с какого падения писать


def _note_crash() -> int:
    """Считает падения в скользящем окне. Возвращает их число.

    Одиночное падение мост лечит сам — беспокоить незачем. Пишем только когда
    падения повторяются, т.е. самолечение НЕ помогает и нужен разбор."""
    now = time.time()
    stamps = []
    try:
        with open(CRASH_FILE) as f:
            stamps = [float(x) for x in f.read().split() if x]
    except Exception:
        pass
    stamps = [t for t in stamps if now - t < CRASH_WINDOW]
    stamps.append(now)
    try:
        with open(CRASH_FILE, "w") as f:
            f.write(" ".join(str(t) for t in stamps))
    except Exception:
        pass
    return len(stamps)


async def _die(reason: str, code: int = 1):
    """Аварийный выход с попыткой чисто закрыть браузер (иначе persistent-профиль
    остаётся залоченным SingletonLock и следующий старт падает). systemd поднимет.

    ТИХО: обычное самовосстановление пользователя не беспокоит. Сообщение уходит
    только если мост падает повторно — значит сам не выправляется."""
    n = _note_crash()
    log.error("Аварийный выход: %s (падение %d за час)", reason, n)
    if n >= CRASH_ALERT_AT:
        try:
            await asyncio.wait_for(alert_tg(
                f"🚨 Мост падает повторно ({n} раз за час): {reason}.\n"
                f"Самовосстановление не помогает — нужен разбор кода."), 8)
        except Exception:
            pass
    if _ctx is not None:
        try:
            await asyncio.wait_for(_ctx.close(), 5)   # снять SingletonLock профиля
        except Exception:
            pass
    logging.shutdown()
    os._exit(code)


async def composer_present() -> bool:
    """Есть поле ввода => залогинены и беседа открыта."""
    try:
        async with _page_lock:
            return await asyncio.wait_for(
                _page.locator(SEL_COMPOSER).count(), PAGE_OP_TIMEOUT) > 0
    except asyncio.TimeoutError:
        log.error("composer_present: страница зависла (нет ответа за %ss)", PAGE_OP_TIMEOUT)
        raise
    except Exception:
        log.exception("composer_present: не смог опросить страницу")
        return False


def _extract_incoming(msgs: list[dict]) -> list[dict]:
    """Входящие сообщения (текст и/или медиа) в порядке DOM.

    Автор наследуется от предыдущего сообщения серии: в сгруппированных
    сообщениях MAX показывает имя только у первого, у остальных заголовка нет.
    """
    out = []
    last_author = "Кто-то"
    dup_seen: dict[str, int] = {}   # счётчик одинаковых (автор|контент|время) в этом окне
    for m in msgs:
        text = m.get("text") or ""
        media = m.get("media")
        raw_author = (m.get("author") or "").strip()
        # определяем автора с учётом группировки и карты аватар->имя:
        avatar = m.get("avatar") or ""
        if raw_author:
            author = last_author = raw_author        # есть имя
            if avatar:
                _avatar_remember(avatar, raw_author)  # запоминаем аватар->имя (с лимитом)
        elif avatar and _avatar_to_name.get(avatar):
            author = last_author = _avatar_to_name[avatar]  # имя по аватарке
        elif not m.get("hasAvatar"):
            author = last_author                     # продолжение серии -> наследуем
        else:
            author = last_author = "Кто-то"          # начало серии без имени -> неизвестен
        if not text and not media:
            continue
        if m.get("variant") == "outgoing" and not MIRROR_OUTGOING:
            continue
        tm = m.get("time", "")
        # содержимое для дедупа: текст, иначе идентификатор медиа
        content = text or (media.get("id") if media else "")
        # дизамбигуация дублей: N-е одинаковое сообщение получает суффикс #N.
        # Порядок в DOM стабилен между опросами, поэтому sig остаётся устойчивым.
        base = f"{author}|{content}|{tm}"
        n = dup_seen.get(base, 0)
        dup_seen[base] = n + 1
        sig_content = content if n == 0 else f"{content}#{n}"
        out.append({
            "key": f"{author}|{tm}",
            "author": author,
            "text": text,
            "meta_time": tm,          # строка из .meta как есть (ЧЧ:ММ или дата)
            "media": media,
            "reply": m.get("reply"),
            "sig": _sig(author, sig_content, tm),
        })
    return out


_HHMM_ONLY = re.compile(r"^\d{1,2}:\d{2}$")


def bootstrap_resend_set(incoming: list, last_n: int) -> set:
    """Подписи сообщений, которые на первом запуске НЕ глушим, а доставляем.

    Берём не более last_n последних и только те, у которых в .meta время
    ЧЧ:ММ — такие сообщения сегодняшние и могли прийти в момент переключения.
    У старых сообщений в .meta дата, их исключаем: дубликаты не нужны.
    """
    if last_n <= 0 or not incoming:
        return set()
    tail = incoming[-last_n:]
    return {m["sig"] for m in tail
            if _HHMM_ONLY.match((m.get("meta_time") or "").strip())}


def prime_or_resume(incoming: list) -> tuple:
    """Решает, что делать при инициализации: первый запуск или возобновление.

    Возвращает (prev_visible, режим, сколько_восстановлено).
    Вынесено из poll_max_loop, чтобы это можно было проверить тестом без браузера.

    ПЕРВЫЙ ЗАПУСК: историю беседы в Telegram не сливаем — помечаем доставленной.
    ВОЗОБНОВЛЕНИЕ: поднимаем окно прошлого опроса из базы, дальше обычный
    classify() увидит всё, что появилось за простой, а база не даст
    продублировать уже доставленное.
    """
    if _store is None:
        return list(incoming), "без-базы", 0
    if not _store.is_bootstrapped():
        resend = bootstrap_resend_set(incoming, BOOTSTRAP_RESEND_LAST)
        for m in incoming:
            if m["sig"] in resend:
                # контрольное окно: могло прийти в момент переключения
                _store.record_detected("new", m, TELEGRAM_TARGET_CHAT_IDS)
                _seen_add(m["sig"])
            else:
                _store.record_silent("new", m, TELEGRAM_TARGET_CHAT_IDS)
                _seen_add(m["sig"])
                _forwarded_add(m["sig"])
        _store.mark_bootstrapped()
        _store.save_window(incoming)
        return list(incoming), "первый-запуск", len(resend)
    restored = _store.load_window()
    return (restored or list(incoming)), "возобновление", len(restored)


async def poll_max_loop():
    global _primed, _prev_visible, _last_window_key, _deliver_skip
    missing = 0
    recover_tried = False
    alerted = False
    prime_wait = 0.0
    qr_sent_at = -1
    # счётчики для heartbeat: без них по логу не отличить "молчит беседа" от "мост завис"
    stats = {"new": 0, "edit": 0, "del": 0}
    # heartbeat ПО ЧАСАМ, а не по числу циклов: длительность цикла плавает
    # (прокрутка + чтение DOM), и счёт в циклах растягивал интервал за порог
    # внешнего сторожа -> тот перезапускал живой мост и слал алерты (спам).
    last_hb = time.monotonic()
    empty_streak = 0            # подряд опросов "composer есть, но 0 сообщений"
    stale_alerted = False

    while True:
        try:
            _touch_alive()   # пульс: цикл крутится (не зависит от журнала и активности беседы)

            # --- вотчдог браузера: мёртвый браузер лечится только рестартом ---
            if not browser_alive():
                await _die("браузер упал")   # не вернётся

            # --- вотчдог сессии ---
            if not await composer_present():
                missing += 1
                secs = missing * POLL_SEC
                if secs >= 15 and not recover_tried:
                    recover_tried = True
                    log.warning("Поле ввода пропало — пробую переоткрыть беседу")
                    try:
                        async with _page_lock:
                            await asyncio.wait_for(
                                _page.goto(MAX_URL, wait_until="domcontentloaded"),
                                PAGE_OP_TIMEOUT)
                        # DOM полностью перезагружен — старый снимок несопоставим,
                        # переинициализируем окно молча, иначе залп ложных new/delete.
                        _primed = False
                        _prev_visible = []
                        log.info("Беседа переоткрыта — переинициализирую окно")
                    except Exception:
                        log.warning("Не удалось переоткрыть беседу")
                # Молчим первые SESSION_GRACE секунд: короткий обрыв мост чинит сам
                # (переоткрытие беседы выше), беспокоить незачем.
                elif secs >= SESSION_GRACE and not alerted:
                    alerted = True
                    await alert_tg("⚠️ MAX: сессия отвалилась — нужен ТЫ.\n"
                                   "Отсканируй QR в приложении MAX "
                                   "(Настройки → Устройства).")
                    await send_login_screenshot()
                    qr_sent_at = secs
                # напоминаем редко (раз в 30 мин), пока не вернёмся
                elif alerted and secs - qr_sent_at >= QR_REPEAT_SEC:
                    qr_sent_at = secs
                    await send_login_screenshot()
                await asyncio.sleep(POLL_SEC)
                continue

            if missing or alerted:
                log.info("Поле ввода снова на месте — продолжаю")
            missing, recover_tried, alerted = 0, False, False

            await scroll_to_latest()      # без этого новые сообщения не попадают в DOM
            msgs = await read_messages()
            incoming = _extract_incoming(msgs)

            if not _primed:
                # ждём, пока список отрисуется, чтобы не слить историю в TG
                if (len(msgs) > 0 and prime_wait >= PRIME_SETTLE) or prime_wait >= 20:
                    _prev_visible, mode, nrest = prime_or_resume(incoming)
                    if mode == "первый-запуск":
                        log.info("Первый запуск: история помечена доставленной без "
                                 "отправки; в контрольное окно поставлено %d "
                                 "сегодняшних сообщений (возможны разовые дубликаты)",
                                 nrest)
                    else:
                        log.info("Возобновление (%s): окно восстановлено (%d), "
                                 "в очереди на доставку %d", mode, nrest,
                                 _store.pending_count() if _store else 0)
                    _primed = True
                    if TEST_FORWARD_LAST and incoming:
                        last = incoming[-1]
                        await forward_to_tg(last["author"], last["text"],
                                            last.get("media"), last.get("reply"))
                else:
                    prime_wait += POLL_SEC
                await asyncio.sleep(POLL_SEC)
                continue

            # детект отвала селекторов: composer есть, а сообщений стабильно 0 —
            # почти наверняка MAX сменил вёрстку и .bubbleContent не матчится.
            if not incoming:
                empty_streak += 1
                if empty_streak >= STALE_ALERT_LOOPS and not stale_alerted:
                    stale_alerted = True
                    log.error("0 сообщений %d опросов подряд — возможно, отвалились селекторы",
                              empty_streak)
                    await alert_tg("⚠️ MAX: беседа открыта, но мост не видит ни одного "
                                   "сообщения. Возможно, MAX сменил вёрстку — нужна "
                                   "рекалибровка селекторов.")
            else:
                empty_streak = 0
                stale_alerted = False

            new, edited, deleted = classify(_prev_visible, incoming, _seen, _forwarded)
            _prev_visible = list(incoming)

            # --- ОБНАРУЖЕНИЕ: только регистрируем в базе, ничего не отправляем.
            # record_detected идемпотентен (INSERT OR IGNORE по event_id = тип:подпись),
            # поэтому повторное чтение того же DOM не создаёт дубликатов.
            if _store is None:
                # АВАРИЙНЫЙ РЕЖИМ (база недоступна): поведение как до внедрения —
                # отправляем сразу, персистентности нет. Лучше так, чем молчать.
                for m in new:
                    _seen_add(m["sig"])
                    if await forward_to_tg(m["author"], m["text"],
                                           m.get("media"), m.get("reply")):
                        _forwarded_add(m["sig"])
                        stats["new"] += 1
                for m in (edited if TRACK_EDITS else []):
                    _seen_add(m["sig"])
                    if await forward_edit(m["author"], m["text"]):
                        _forwarded_add(m["sig"])
                        stats["edit"] += 1
                for m in (deleted if TRACK_DELETES else []):
                    if await forward_delete(m["author"], m["text"]):
                        stats["del"] += 1
                # (в аварийном режиме состояние по чатам не ведётся — как в прежней версии)
            else:
                for m in new:
                    # «Замораживаем» альбом СЕЙЧАС, пока DOM соответствует записи:
                    # при отложенной доставке последний медиа-блок будет уже другим.
                    md = m.get("media")
                    if md and md.get("type") == "photo" and md.get("pending"):
                        try:
                            got = await resolve_album_srcs(md.get("count", 1))
                            if got:
                                md["srcs"] = got
                                md["pending"] = False
                        except Exception:
                            log.warning("не смог заранее разрешить альбом")
                    if _store.record_detected("new", m, TELEGRAM_TARGET_CHAT_IDS):
                        _seen_add(m["sig"])
                for m in (edited if TRACK_EDITS else []):
                    if _store.record_detected("edit", m, TELEGRAM_TARGET_CHAT_IDS):
                        _seen_add(m["sig"])
                for m in (deleted if TRACK_DELETES else []):
                    _store.record_detected("delete", m, TELEGRAM_TARGET_CHAT_IDS)
                # Окно сохраняем ПОСЛЕ регистрации: если процесс умрёт между
                # этими шагами, события уже в базе и будут доставлены.
                # Пишем ТОЛЬКО при изменении окна: иначе это 30 INSERT каждые
                # 2 секунды, то есть ~1.3 млн записей в сутки на диск, который
                # занят на 91 %. В молчащей беседе теперь записей нет вовсе.
                wkey = tuple(m["sig"] for m in incoming)
                if wkey != _last_window_key:
                    try:
                        _store.save_window(incoming)
                        _last_window_key = wkey
                    except Exception:
                        log.exception("не смог сохранить окно в базу")

            # --- ДОСТАВКА: отдельный шаг. Подтверждение = delivered в базе.
            # При полном отказе Telegram шаг пропускается на DELIVER_PAUSE_CYCLES
            # циклов, чтобы не тормозить обнаружение новых сообщений.
            if _deliver_skip > 0:
                _deliver_skip -= 1
            else:
                sent = await deliver_pending()
                stats["new"] += sent["new"]
                stats["edit"] += sent["edit"]
                stats["del"] += sent["del"]
                progress = sent["new"] + sent["edit"] + sent["del"]
                if sent["failed"] and not progress:
                    _deliver_skip = DELIVER_PAUSE_CYCLES
                    log.warning("Telegram не отвечает — пауза доставки на %d циклов "
                                "(%d заявок ждут в очереди)", DELIVER_PAUSE_CYCLES,
                                _store.pending_count() if _store else 0)

            now = time.monotonic()
            if now - last_hb >= HEARTBEAT_SEC:
                last_hb = now
                pend = _store.pending_count() if _store else 0
                by_chat = _store.pending_by_chat() if _store else {}
                log.info("жив: в беседе видно %d сообщений; за сессию доставлено "
                         "новых=%d правок=%d удалений=%d; в очереди %d%s",
                         len(incoming), stats["new"], stats["edit"], stats["del"],
                         pend, f" {by_chat}" if by_chat else "")
                if _store is not None:
                    _store.prune(STATE_KEEP)
        except asyncio.TimeoutError:
            # страница зависла под локом — единственное лечение это рестарт процесса
            await _die("страница зависла (рендерер не отвечает)")   # не вернётся
        except Exception:
            log.exception("poll_max_loop")
        await asyncio.sleep(POLL_SEC)


MEDIA_MAX_BYTES = int(os.environ.get("MEDIA_MAX_BYTES", str(45 * 1024 * 1024)))


async def _tg_retry(make_coro, what: str) -> bool:
    """Вызывает отправку с ретраями — транзиентный сбой TG не теряет сообщение."""
    for attempt in range(max(1, TG_RETRY_ATTEMPTS)):
        try:
            await make_coro()
            return True
        except Exception:
            if attempt < TG_RETRY_ATTEMPTS - 1:
                await asyncio.sleep(0.4 * (attempt + 1))
    log.warning("Не доставил %s после ретраев", what)
    return False


async def _send_targets(html_text: str, targets=None) -> set:
    """Отправляет текст указанным получателям. Возвращает МНОЖЕСТВО чатов,
    которые приняли. Остальные останутся в очереди именно для себя."""
    ok = set()
    for chat_id in (targets if targets is not None else TELEGRAM_TARGET_CHAT_IDS):
        if await _tg_retry(
                lambda cid=chat_id: tg_bot.send_message(cid, html_text,
                                                        parse_mode="HTML"),
                f"текст в chat {chat_id}"):
            ok.add(chat_id)
    return ok


AUDIO_EXT = (".mp3", ".ogg", ".oga", ".m4a", ".wav", ".opus", ".aac", ".flac")


async def _send_bytes_targets(kind: str, data: bytes, filename: str,
                              caption: str, targets=None) -> set:
    def _send(cid):
        f = BufferedInputFile(data, filename=filename)   # свежий объект на попытку
        if kind == "photo":
            return tg_bot.send_photo(cid, f, caption=caption, parse_mode="HTML")
        if kind == "video":
            return tg_bot.send_video(cid, f, caption=caption, parse_mode="HTML")
        if kind == "audio":
            return tg_bot.send_audio(cid, f, caption=caption, parse_mode="HTML")
        return tg_bot.send_document(cid, f, caption=caption, parse_mode="HTML")
    ok = set()
    for chat_id in (targets if targets is not None else TELEGRAM_TARGET_CHAT_IDS):
        if await _tg_retry(lambda cid=chat_id: _send(cid), f"{kind} в chat {chat_id}"):
            ok.add(chat_id)
    return ok


async def _send_photo_group(datas: list, caption: str, targets=None) -> set:
    """Несколько фото одним альбомом в Telegram."""
    def _group(cid):
        group = []
        for i, d in enumerate(datas[:10]):
            group.append(InputMediaPhoto(
                media=BufferedInputFile(d, filename=f"photo{i}.jpg"),
                caption=caption if i == 0 else None, parse_mode="HTML"))
        return tg_bot.send_media_group(cid, group)
    ok = set()
    for chat_id in (targets if targets is not None else TELEGRAM_TARGET_CHAT_IDS):
        if await _tg_retry(lambda cid=chat_id: _group(cid), f"альбом в chat {chat_id}"):
            ok.add(chat_id)
    return ok


async def resolve_album_srcs(expected: int) -> list:
    """Дожидается ленивой загрузки фото в последнем альбоме (скролл + ожидание)."""
    if not _page:
        return []
    try:
        async with _page_lock:
            return await _page.evaluate("""async (expected) => {
                const boxes = [...document.querySelectorAll('.bubbleContent .media, .bubbleContent .attaches')];
                const box = boxes[boxes.length - 1];
                if (!box) return [];
                box.scrollIntoView({block: 'center'});
                for (let t = 0; t < 14; t++) {
                    const s = [...box.querySelectorAll('img.image')]
                        .map(i => i.src).filter(x => x && !x.includes('/emojis/'));
                    if (s.length >= expected && s.length > 0) return s;
                    await new Promise(r => setTimeout(r, 400));
                }
                return [...box.querySelectorAll('img.image')]
                    .map(i => i.src).filter(x => x && !x.includes('/emojis/'));
            }""", expected)
    except Exception:
        log.warning("resolve_album_srcs не смог")
        return []


async def download_file(filename: str):
    """Качает файл кликом по карточке (перехват download) -> (bytes, имя)."""
    if not _page:
        return None, None
    dl = None
    try:
        async with _page_lock:
            card = _page.get_by_role("button").filter(has_text=filename).last
            async with _page.expect_download(timeout=20000) as di:
                await card.click(timeout=6000)
            dl = await di.value
            path = await dl.path()
            name = dl.suggested_filename or filename
        # РАЗМЕР ПРОВЕРЯЕМ ДО ЧТЕНИЯ: иначе вложение на сотни МБ целиком уедет
        # в память (при 956 МБ RAM это гарантированный OOM всего сервера).
        size = os.path.getsize(path)
        if size > MEDIA_MAX_BYTES:
            log.info("download_file: файл %d байт больше лимита — заглушка", size)
            return None, None
        with open(path, "rb") as f:            # без утечки дескриптора
            data = f.read()
        return data, name
    except Exception:
        log.warning("download_file не смог скачать '%s'", filename)
        return None, None
    finally:
        if dl is not None:
            try:
                await dl.delete()              # чистим временный файл (иначе копится на диске)
            except Exception:
                pass


async def screenshot_element(selector: str):
    """PNG-снимок элемента (для стикеров-lottie): скролл к нему + пауза на отрисовку."""
    if not _page:
        return None
    try:
        async with _page_lock:
            loc = _page.locator(selector).last
            await loc.scroll_into_view_if_needed(timeout=5000)
        await asyncio.sleep(1.3)                           # даём lottie кадр БЕЗ лока
        async with _page_lock:
            # сначала пробуем прочитать пиксели canvas напрямую
            b64 = await _page.evaluate("""(sel) => {
                const el = [...document.querySelectorAll(sel)].pop();
                const cv = el && el.querySelector('canvas');
                if (!cv) return null;
                try { return cv.toDataURL('image/png').split(',')[1]; } catch(e) { return null; }
            }""", selector)
        if b64:
            import base64
            data = base64.b64decode(b64)
            if len(data) > 2000:                           # не пустой кадр
                return data
        async with _page_lock:                             # запасной путь — снимок элемента
            return await _page.locator(selector).last.screenshot(timeout=6000)
    except Exception:
        log.warning("screenshot_element не смог: %s", selector)
        return None


async def fetch_bytes(url: str) -> bytes | None:
    """Качает URL через request-контекст браузера (куки сессии, БЕЗ CORS) -> байты."""
    if not _page or not url:
        return None
    resp = None
    try:
        resp = await _page.context.request.get(url, timeout=25000)
        if not resp.ok:
            log.warning("медиа: HTTP %s при скачивании", resp.status)
            return None
        # размер по заголовку ДО загрузки тела — иначе большое видео = OOM на 956МБ RAM
        clen = resp.headers.get("content-length")
        if clen and clen.isdigit() and int(clen) > MEDIA_MAX_BYTES:
            log.info("медиа слишком большое по заголовку (%s байт) — заглушка", clen)
            return None
        data = await resp.body()
        if len(data) > MEDIA_MAX_BYTES:
            log.info("медиа слишком большое (%d байт) — заглушка", len(data))
            return None
        return data
    except Exception:
        log.warning("fetch_bytes не смог скачать медиа")
        return None
    finally:
        if resp is not None:
            try:
                await resp.dispose()      # освобождаем буфер тела (иначе утечка на месяцы)
            except Exception:
                pass


def _quote(reply: dict) -> str:
    """Префикс-цитата для сообщения-ответа (реплая)."""
    if not reply:
        return ""
    qa = (reply.get("author") or "").strip()
    qt = (reply.get("text") or "").strip()
    if not qa and not qt:
        return ""
    inner = (f"<b>{html.escape(qa)}</b>: " if qa else "") + html.escape(qt[:200])
    return f"<blockquote>↩️ {inner}</blockquote>\n"


def _head(author: str, text: str, reply: dict = None, limit: int = TG_TEXT_LIMIT) -> str:
    h = _quote(reply) + f"👤 <b>{html.escape(author)}</b>"
    # режем сырой текст ДО экранирования, иначе можно разрезать HTML-сущность (&lt; -> &l)
    return h + (f"\n{html.escape(text[:limit])}" if text else "")


# запас длины текста под медиа-подпись, чтобы HTML-caption влез в лимит Telegram (1024)
MEDIA_CAPTION_TEXT = 700


async def forward_to_tg(author: str, text: str, media: dict = None,
                        reply: dict = None, allow_dom: bool = True,
                        targets=None) -> set:
    """allow_dom=False — доставка отложенная, DOM уже не соответствует записи:
    к браузеру не обращаемся, отдаём текст с пометкой о вложении."""
    if not media:
        return await _send_targets(_head(author, text, reply), targets)
    cap = _head(author, text, reply, limit=MEDIA_CAPTION_TEXT)   # подпись ≤1024 у Telegram
    mtype = media.get("type")

    if mtype == "photo":
        srcs = list(media.get("srcs") or ([media["src"]] if media.get("src") else []))
        if (media.get("pending") or len(srcs) < media.get("count", 1)) and allow_dom:
            srcs = await resolve_album_srcs(media.get("count", 1)) or srcs
        datas = []
        budget = ALBUM_MAX_BYTES
        for s in srcs[:10]:
            d = await fetch_bytes(s)
            if d:
                budget -= len(d)
                if budget < 0:                 # не держим весь тяжёлый альбом в RAM (OOM)
                    log.info("альбом превысил бюджет памяти — часть фото пропущена")
                    break
                datas.append(d)
        if len(datas) == 1:
            return await _send_bytes_targets("photo", datas[0], "photo.jpg", cap, targets)
        if len(datas) > 1:
            return await _send_photo_group(datas, cap, targets)

    elif mtype == "video":
        data = await fetch_bytes(media.get("src"))
        if data:
            return await _send_bytes_targets("video", data, "video.mp4", cap, targets)
        poster = await fetch_bytes(media.get("poster"))       # запасной кадр
        if poster:
            return await _send_bytes_targets("photo", poster, "poster.jpg", cap + "\n🎥 видео", targets)

    elif mtype in ("file", "audio"):
        if media.get("src"):                                   # голосовое/аудио-плеер с src
            data = await fetch_bytes(media["src"])
            if data:
                return await _send_bytes_targets("audio", data, "audio.ogg", cap, targets)
        data, fname = (await download_file(media.get("filename") or "file")
                       if allow_dom else (None, None))      # карточка файла — только по свежему DOM
        if data:
            kind = "audio" if fname.lower().endswith(AUDIO_EXT) else "document"
            return await _send_bytes_targets(kind, data, fname, cap, targets)

    elif mtype == "sticker":
        sid = media.get("id") or ""
        png = (await screenshot_element(f'[data-testid="{sid}"]')
               if allow_dom else None)
        if png:
            return await _send_bytes_targets("photo", png, "sticker.png", cap + "\n🎨 стикер", targets)

    # крайний случай, если байты достать не удалось
    fname = html.escape(media.get("filename", ""))   # имя из беседы -> экранируем в HTML
    labels = {"video": "🎥 [видео]", "file": f"📎 [файл: {fname}]",
              "audio": "🎤 [аудио/голосовое]", "sticker": "🎨 [стикер]"}
    tail = labels.get(mtype, "📎 [вложение]")
    if not allow_dom:
        tail += " — вложение не удалось получить (доставка отложена, беседа уже ушла вперёд)"
    return await _send_targets(cap + "\n" + tail, targets)


async def forward_edit(author: str, text: str, targets=None) -> set:
    return await _send_targets(
        f"✏️ <b>{html.escape(author)}</b> изменил(а):\n"
        f"{html.escape(text)[:TG_TEXT_LIMIT]}", targets)


async def forward_delete(author: str, text: str, targets=None) -> set:
    body = html.escape(text)[:TG_TEXT_LIMIT] if text else "вложение"
    return await _send_targets(
        f"🗑 <b>{html.escape(author)}</b> удалил(а):\n<s>{body}</s>", targets)


async def deliver_pending() -> dict:
    """Отправляет то, что лежит в очереди, и подтверждает доставку ПО КАЖДОМУ ЧАТУ.

    Разделение «обнаружил» и «доставил» — ядро защиты от потерь: пара
    (событие, чат) помечается delivered ТОЛЬКО после успешной отправки именно
    в этот чат. Чат, который уже получил, повторно не беспокоим. Чат, который
    не получил, остаётся в очереди для себя и повторяется с нарастающей
    задержкой, в том числе ПОСЛЕ перезапуска процесса.

    Медиа скачивается ОДИН раз на все недоставленные чаты — лишней нагрузки на
    сервер отдельное состояние доставки не создаёт.
    """
    global _giveup_alerted
    done = {"new": 0, "edit": 0, "del": 0, "failed": 0}
    if _store is None:
        return done
    deadline = time.monotonic() + DELIVER_BUDGET_SEC
    claimed = _store.claim_pending(DELIVER_BATCH)
    for i, row in enumerate(claimed):
        kind, sig, eid = row["kind"], row["sig"], row["event_id"]
        targets = row.get("targets") or []
        if not targets:
            continue
        if time.monotonic() > deadline:
            # бюджет исчерпан: остаток возвращаем в очередь БЕЗ штрафа,
            # чтобы не держать цикл опроса и не терять заявки в sending
            for rest in claimed[i:]:
                _store.release(rest["event_id"], rest.get("targets") or [])
            log.info("бюджет доставки исчерпан — %d заявок перенесено на "
                     "следующий цикл", len(claimed) - i)
            break
        try:
            if kind == "edit":
                ok = await forward_edit(row["author"], row["text"], targets=targets)
            elif kind == "delete":
                ok = await forward_delete(row["author"], row["text"], targets=targets)
            else:
                fresh = (time.time() - row.get("first_seen", 0)) <= DOM_FRESH_SEC
                ok = await forward_to_tg(row["author"], row["text"],
                                         row["media"], row["reply"],
                                         allow_dom=fresh, targets=targets)
        except Exception as e:                      # сетевой сбой, таймаут и т.п.
            log.warning("доставка %s не удалась (%s) — повторю позже", kind, e)
            for chat in targets:
                _store.mark_failed(eid, chat, repr(e))
            done["failed"] += 1
            continue
        ok = set(ok or ())
        for chat in targets:
            if chat in ok:
                _store.mark_delivered(eid, chat)
            else:
                state = _store.mark_failed(eid, chat,
                                           "Telegram не подтвердил отправку")
                done["failed"] += 1
                if state == "giveup" and not _giveup_alerted:
                    _giveup_alerted = True
                    await alert_tg("🚨 Сообщение не удаётся доставить в один из "
                                   "чатов после многих попыток. Данные сохранены, "
                                   "ничего не потеряно — нужен разбор.")
        if ok:
            _forwarded_add(sig)                     # для детекта правок/удалений
            key = {"new": "new", "edit": "edit", "delete": "del"}[kind]
            done[key] += 1
            log.info("→TG %s | %s | %s%s | чаты: %s%s", kind, row["author"],
                     (row.get("media") or {}).get("type", "текст"),
                     " | " + row["text"][:50] if row["text"] else "",
                     sorted(ok),
                     f" (ждут: {sorted(set(targets) - ok)})"
                     if set(targets) - ok else "")
    return done


async def alert_tg(text: str):
    if not TELEGRAM_TARGET_CHAT_IDS:
        return
    try:
        await tg_bot.send_message(TELEGRAM_TARGET_CHAT_IDS[0], text)
    except Exception:
        log.warning("Не смог отправить служебный алерт в TG")


async def send_login_screenshot():
    """Снимок страницы (с QR) -> в Telegram, чтобы перелогиниться без доступа к серверу."""
    if not TELEGRAM_TARGET_CHAT_IDS or not _page:
        return
    try:
        async with _page_lock:
            data = await asyncio.wait_for(
                _page.screenshot(full_page=False), PAGE_OP_TIMEOUT)   # байты, не файл на диск
    except Exception:
        # снимок не сняли (обычно браузер мёртв) — молчать нельзя, зовём текстом
        log.exception("Не смог СНЯТЬ скриншот QR")
        await alert_tg("⚠️ MAX: сессия слетела, но снять QR не удалось "
                       "(браузер не отвечает). Перезапускаю мост.")
        return
    try:
        await tg_bot.send_photo(
            TELEGRAM_TARGET_CHAT_IDS[0],
            BufferedInputFile(data, filename="login_qr.png"),   # из памяти, без файла в cwd
            caption="Отсканируй этот QR в приложении MAX (Настройки → Устройства).",
        )
    except Exception:
        log.exception("Не смог ОТПРАВИТЬ скриншот QR в TG")


# ---------------------------------------------------------------------------
# Кнопка "Скриншот чата" (работает всегда, даже когда запись в MAX выключена)
# ---------------------------------------------------------------------------
BTN_SHOT = "📸 Скриншот чата"
_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=BTN_SHOT)]],
    resize_keyboard=True, is_persistent=True,
    input_field_placeholder="Нажми кнопку — пришлю снимок беседы",
)


def _allowed(message: TgMessage) -> bool:
    # fail-CLOSED: пустой вайтлист = никому (иначе посторонний получит скриншот беседы).
    uid = message.from_user.id if message.from_user else None
    return bool(TELEGRAM_ALLOWED_USER_IDS) and uid in TELEGRAM_ALLOWED_USER_IDS


async def send_chat_screenshot(chat_id: int):
    """Полноразмерный снимок открытой беседы MAX -> в TG документом (без сжатия)."""
    if not _page or _page.is_closed():
        await tg_bot.send_message(chat_id, "⚠️ Браузер не отвечает — снимок недоступен, "
                                           "перезапускаю мост.")
        return
    # снимок должен быть со СВЕЖИМИ сообщениями: крутим до упора вниз
    at_bottom = await scroll_to_latest(passes=6)
    await asyncio.sleep(1.0)                  # дать новым пузырям отрисоваться
    if not at_bottom:
        log.warning("send_chat_screenshot: не докрутил до низа беседы")
    try:
        async with _page_lock:
            data = await _page.screenshot(full_page=False)   # bytes, текущий вид беседы
    except Exception:
        log.exception("send_chat_screenshot: не смог снять")
        await tg_bot.send_message(chat_id, "⚠️ Не удалось снять скриншот (см. логи).")
        return
    # документом -> Telegram не сжимает, текст переписки читается в полном размере
    await tg_bot.send_document(
        chat_id,
        BufferedInputFile(data, filename="max_chat.png"),
        caption="Снимок беседы MAX (полный размер — открой файл).",
    )


@tg_dp.message(CommandStart())
async def on_start(message: TgMessage):
    if not _allowed(message):
        return
    await message.answer(
        "Мост MAX→Telegram на связи.\nНажми кнопку, чтобы получить снимок беседы:",
        reply_markup=_kb,
    )


@tg_dp.message(Command("screenshot"))
async def on_shot_cmd(message: TgMessage):
    if not _allowed(message):
        return
    await send_chat_screenshot(message.chat.id)


@tg_dp.message(F.text == BTN_SHOT)
async def on_shot_btn(message: TgMessage):
    if not _allowed(message):
        return
    await send_chat_screenshot(message.chat.id)


# ---------------------------------------------------------------------------
# Telegram -> MAX
# ---------------------------------------------------------------------------
@tg_dp.message()
async def on_tg(message: TgMessage):
    if not SEND_TO_MAX:                       # главная защита
        return
    if not _allowed(message):                 # fail-closed вайтлист
        return
    uid = message.from_user.id if message.from_user else None
    raw = (message.text or message.caption or "")
    # все разделители строк -> пробел (иначе Enter в composer отправит недописанное),
    # плюс ограничение длины.
    body = re.sub(r"[\r\n\u2028\u2029]+", " ", raw).strip()[:MAX_SEND_LEN]
    if not body or not _page:
        return
    name = SIGN_MAP.get(uid) or (message.from_user.first_name if message.from_user else "")
    name = re.sub(r"[\r\n\u2028\u2029]+", " ", name).strip()
    payload = f"{name}: {body}" if name else body
    try:
        async with _page_lock:
            box = _page.locator(SEL_COMPOSER)
            await box.click(timeout=5000)
            await _page.keyboard.type(payload)
            await _page.keyboard.press("Enter")
    except Exception:
        log.exception("on_tg: не смог напечатать в MAX")
        try:
            await message.reply("⚠️ Не отправлено в MAX (см. логи)")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Калибровка
# ---------------------------------------------------------------------------
async def calibrate():
    wait = int(os.environ.get("CALIB_WAIT", "180"))
    log.info("CALIBRATE: залогинься по QR и ОТКРОЙ нужную беседу. Жду %d сек...", wait)
    await asyncio.sleep(wait)
    html_content = await _page.content()
    with open("calib_dump.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    log.info("Сохранил calib_dump.html (%d байт).", len(html_content))


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def open_state_store():
    """Открывает базу состояния и восстанавливает всё, что не доехало.

    Вызывается ДО запуска браузера: если база битая, лучше узнать сразу."""
    global _store
    try:
        _store = StateStore(STATE_DB)
    except Exception:
        # Диск заполнен или база повреждена. Мост ДОЛЖЕН работать и без базы:
        # отсутствие персистентности хуже, чем полная остановка зеркала.
        log.exception("НЕ СМОГ открыть базу состояния %s — работаю в аварийном "
                      "режиме БЕЗ персистентности (как до внедрения)", STATE_DB)
        _store = None
        return
    stuck = _store.recover_stuck_sending()
    if stuck:
        log.warning("Найдено %d отправок, прерванных на полпути — "
                    "возвращены в очередь (возможен дубликат, но не потеря)", stuck)
    # прогреваем кэши для classify() из базы — это и есть персистентность
    for sig in _store.load_seen(SEEN_MAX):
        _seen_add(sig)
    for sig in _store.load_forwarded(SEEN_MAX):
        _forwarded_add(sig)
    _store.prune(STATE_KEEP)
    st = _store.stats()
    log.info("Состояние загружено: доставлено=%s в очереди=%s окно=%s "
             "(первый запуск: %s)",
             st.get("delivered", 0), _store.pending_count(), st.get("window", 0),
             "нет" if st.get("bootstrapped") else "ДА")


async def main():
    global _page, _ctx
    check_config()
    open_state_store()
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=not HEADFUL,
            viewport={"width": 1280, "height": 900},
            accept_downloads=True,
            args=["--disable-blink-features=AutomationControlled", *CHROMIUM_ARGS],
        )
        _ctx = ctx                      # для чистого закрытия профиля в _die (SingletonLock)
        _page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        await _page.goto(MAX_URL, wait_until="domcontentloaded")
        log.info("Открыл %s", MAX_URL)

        if CALIBRATE:
            await calibrate()
            await ctx.close()
            return

        if TEST_READ:
            wait = int(os.environ.get("TEST_WAIT", "45"))
            log.info("TEST_READ: открой беседу, жду %d сек...", wait)
            await asyncio.sleep(wait)
            msgs = await read_messages()
            log.info("Разобрано сообщений: %d", len(msgs))
            for m in msgs[-15:]:
                log.info("[%s] %s (%s): %s", m.get("variant"), m.get("author") or "—",
                         m.get("time"), m.get("text"))
            await ctx.close()
            return

        await tg_bot.delete_webhook(drop_pending_updates=True)
        log.info("SEND_TO_MAX=%s (запись в беседу %s)",
                 SEND_TO_MAX, "ВКЛ" if SEND_TO_MAX else "ВЫКЛ")

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(s, stop.set)
            except NotImplementedError:
                pass

        tasks = [
            asyncio.create_task(poll_max_loop()),
            # handle_signals=False: завершением управляет наш обработчик ниже,
            # иначе aiogram перехватывает SIGTERM и процесс зависает при остановке
            asyncio.create_task(tg_dp.start_polling(tg_bot, handle_signals=False)),
        ]
        try:
            await stop.wait()
        finally:
            log.info("Останавливаюсь, закрываю браузер...")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await ctx.close()
            except Exception:
                pass
            if _store is not None:
                _store.close()


if __name__ == "__main__":
    asyncio.run(main())
