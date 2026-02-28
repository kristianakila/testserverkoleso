import os
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from html import escape

from flask import Flask, request, jsonify, g, send_from_directory, abort, make_response
from flask_cors import CORS  # Добавляем CORS
import asyncio
from telegram import Bot
from telegram.error import TelegramError, Forbidden, BadRequest
import threading
import time
import re

# ==== ПЕРЕМЕННЫЕ (Студия BODYFACEROOM) ====
BOT_TOKEN  = os.environ.get('BOT_TOKEN', '8275038606:AAEubsCRXwU4xnQfolCTr0jCwHqH7ZRxgxA')
SUBSCRIPTION_CHANNEL_ID = os.environ.get('SUBSCRIPTION_CHANNEL_ID', '-1001698393800')
LEADS_TARGET_ID         = os.environ.get('LEADS_TARGET_ID', '-1003413060996')
BOT_USERNAME = os.environ.get('BOT_USERNAME', 'BODYFACEROOMbot').strip('@')

# Админы
_admin_env = os.environ.get(
    'ADMIN_USER_IDS',
    '291655689,463942971,5230371449'
).replace(' ', '')
ADMIN_USER_IDS = [int(x) for x in _admin_env.split(',') if x]

TZ = timezone.utc

# Лимиты
BASE_ATTEMPTS_PER_DAY = int(os.environ.get('BASE_ATTEMPTS_PER_DAY', '2'))
REFERRAL_BONUS        = int(os.environ.get('REFERRAL_BONUS', '1'))

# Периоды
SWEEP_INTERVAL_SECONDS = int(os.environ.get('SWEEP_INTERVAL_SECONDS', '60'))
FALLBACK_TTL_SECONDS   = int(os.environ.get('FALLBACK_TTL_SECONDS', '120'))

# ==== БАЗОВЫЕ ПРИЗЫ ====
PRIZES = [
    'Годовой абонемент на лазерную эпиляцию (подмышки)',
    'Сертификат на 15 000 ₽ на любые услуги',
    'Курс из 10 сеансов LPG-массажа',
    'Скидка 25% на любую одну услугу',
    'Массаж лица в подарок',
    'Купон на 1500 ₽',
    'Скидка 30% для подруги',
    'Скидка 20% на любой абонемент',
    'Альгинатная маска для лица в подарок',
    'Тест-драйв одного любого аппаратного массажа в подарок'
]
PRIZE_WEIGHTS = [10]*10

def weighted_choice(items, weights):
    assert len(items) == len(weights), "Длина items и weights должна совпадать"
    total = sum(max(0, w) for w in weights)
    if total <= 0:
        return random.choice(items)
    r = random.uniform(0, total)
    acc = 0.0
    for item, w in zip(items, weights):
        w = max(0, w)
        acc += w
        if r <= acc:
            return item
    return items[-1]

# Инициализация бота
bot = Bot(token=BOT_TOKEN)

app = Flask(__name__, static_folder='static', static_url_path='')

# ==== НАСТРОЙКА CORS (УЛУЧШЕННАЯ) ====
# Разрешаем все необходимые домены
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "https://neyrolab.ru",
            "http://neyrolab.ru",
            "https://www.neyrolab.ru",
            "http://www.neyrolab.ru",
            "https://web.telegram.org",
            "https://t.me",
            "http://localhost",
            "http://localhost:3000",
            "http://127.0.0.1",
            "http://127.0.0.1:3000",
            "*"  # временно разрешаем все для теста
        ],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "X-Requested-With", "Accept"],
        "expose_headers": ["Content-Type", "X-Requested-With"],
        "supports_credentials": True,
        "max_age": 3600
    }
})

# Добавляем middleware для принудительной установки CORS-заголовков
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response

# Обработчик OPTIONS запросов для всех маршрутов
@app.route('/api/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    response = make_response()
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Max-Age', '3600')
    return response

# Явный обработчик OPTIONS для корневого api
@app.route('/api/<path:path>', methods=['OPTIONS'])
def handle_all_options(path):
    response = make_response()
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Max-Age', '3600')
    return response

DB_PATH = os.path.join(os.path.dirname(__file__), 'app.db')

def get_db():
    db = getattr(g, '_db', None)
    if db is None:
        db = g._db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode = WAL;")
        db.execute("PRAGMA busy_timeout = 5000;")
    return db

@app.teardown_appcontext
def close_db(e=None):
    if hasattr(g, '_db'):
        g._db.close()

def ensure_schema():
    db = get_db()
    db.execute("PRAGMA journal_mode = WAL;")
    db.executescript("""
    CREATE TABLE IF NOT EXISTS spins (
      id       INTEGER PRIMARY KEY,
      user_id  INTEGER NOT NULL,
      ts       TIMESTAMP NOT NULL,
      prize    TEXT
    );
    CREATE TABLE IF NOT EXISTS referrals (
      id           INTEGER PRIMARY KEY,
      referrer_id  INTEGER NOT NULL,
      referred_id  INTEGER NOT NULL,
      ts           TIMESTAMP NOT NULL,
      UNIQUE(referrer_id, referred_id)
    );
    CREATE TABLE IF NOT EXISTS leads (
      user_id   INTEGER PRIMARY KEY,
      username  TEXT,
      name      TEXT,
      phone     TEXT,
      ts        TIMESTAMP NOT NULL
    );
    CREATE TABLE IF NOT EXISTS lead_events (
      spin_id   INTEGER PRIMARY KEY,
      user_id   INTEGER NOT NULL,
      type      TEXT NOT NULL,
      ts        TIMESTAMP NOT NULL
    );
    CREATE TABLE IF NOT EXISTS audience (
      user_id   INTEGER PRIMARY KEY,
      username  TEXT,
      added_at  TIMESTAMP NOT NULL
    );
    CREATE TABLE IF NOT EXISTS wheel_items (
      id       INTEGER PRIMARY KEY,
      pos      INTEGER NOT NULL,
      label    TEXT NOT NULL,
      win_text TEXT,
      weight   INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS pending_fallbacks (
      spin_id    INTEGER PRIMARY KEY,
      user_id    INTEGER NOT NULL,
      prize      TEXT,
      username   TEXT,
      created_ts TIMESTAMP NOT NULL,
      due_ts     TIMESTAMP NOT NULL,
      state      TEXT NOT NULL DEFAULT 'pending'
    );
    CREATE TABLE IF NOT EXISTS broadcast_jobs (
      id               INTEGER PRIMARY KEY,
      created_at       TIMESTAMP NOT NULL,
      created_by       INTEGER,
      text             TEXT NOT NULL,
      parse_mode       TEXT,
      attach_ref       INTEGER NOT NULL DEFAULT 0,
      photo_name       TEXT,
      total_recipients INTEGER NOT NULL DEFAULT 0,
      sent_count       INTEGER NOT NULL DEFAULT 0,
      skipped_count    INTEGER NOT NULL DEFAULT 0,
      error_count      INTEGER NOT NULL DEFAULT 0,
      status           TEXT NOT NULL DEFAULT 'pending'
    );
    CREATE TABLE IF NOT EXISTS broadcast_items (
      id       INTEGER PRIMARY KEY,
      job_id   INTEGER NOT NULL,
      user_id  INTEGER NOT NULL,
      state    TEXT NOT NULL DEFAULT 'pending',
      error    TEXT,
      FOREIGN KEY(job_id) REFERENCES broadcast_jobs(id) ON DELETE CASCADE
    );
    """)
    cols = [r['name'] for r in db.execute("PRAGMA table_info(spins)").fetchall()]
    if 'prize' not in cols:
        db.execute("ALTER TABLE spins ADD COLUMN prize TEXT;")
    db.commit()

with app.app_context():
    ensure_schema()

def utcnow():
    return datetime.now(tz=TZ)

def user_subscribed(user_id: int) -> bool:
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            member = loop.run_until_complete(bot.get_chat_member(chat_id=SUBSCRIPTION_CHANNEL_ID, user_id=user_id))
            return member.status not in ('left', 'kicked')
        finally:
            loop.close()
    except Exception as e:
        print("check_subscribe error:", repr(e))
        return False

def build_ref_link(user_id: int) -> str:
    code = f"uid_{user_id}"
    return f"https://t.me/{BOT_USERNAME}?startapp={code}"

def get_status_for(user_id: int):
    db = get_db()
    total_spins = db.execute(
        "SELECT COUNT(*) AS cnt FROM spins WHERE user_id=?",
        (user_id,)
    ).fetchone()['cnt']
    total_referrals = db.execute(
        "SELECT COUNT(*) AS cnt FROM referrals WHERE referrer_id=?",
        (user_id,)
    ).fetchone()['cnt']
    base_attempts = BASE_ATTEMPTS_PER_DAY
    attempts_granted = base_attempts + REFERRAL_BONUS * total_referrals
    attempts_left = max(0, attempts_granted - total_spins)
    ref_link = build_ref_link(user_id)
    return {
        'attempts_left': attempts_left,
        'bonus': total_referrals,
        'spins_today': total_spins,
        'ref_link': ref_link
    }

def load_wheel_from_db():
    db = get_db()
    rows = db.execute("SELECT label, weight, win_text FROM wheel_items ORDER BY pos ASC").fetchall()
    if not rows:
        return None
    return [(r['label'], int(r['weight'] or 0), r['win_text'] or '') for r in rows]

# Функции для отправки сообщений
def send_telegram_message(chat_id: int, text: str, parse_mode: str = 'HTML'):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode))
        finally:
            loop.close()
    except Exception as e:
        print('send message error:', repr(e))

def send_telegram_photo(chat_id: int, photo_path: str, caption: str, parse_mode: str = 'HTML'):
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            with open(photo_path, 'rb') as f:
                loop.run_until_complete(bot.send_photo(chat_id=chat_id, photo=f, caption=caption, parse_mode=parse_mode))
        finally:
            loop.close()
    except Exception as e:
        print('send photo error:', repr(e))

def _send_fallback_message_direct(user_id: int, spin_id: int, prize: str, username: str):
    text = (
        f"<b>📥 Лид (без телефона)</b>\n"
        f"SpinID: <code>{spin_id}</code>\n"
        f"UserID: <code>{user_id}</code>\n"
        f"Username: @{escape(username) if username else '—'}\n"
        f"Имя: —\n"
        f"Телефон: —\n"
        f"Результат: {escape(prize)}"
    )
    send_telegram_message(chat_id=LEADS_TARGET_ID, text=text, parse_mode='HTML')

def enqueue_fallback(spin_id: int, user_id: int, prize: str, username: str, delay: int = FALLBACK_TTL_SECONDS):
    db = get_db()
    now = utcnow()
    due = now + timedelta(seconds=max(1, int(delay)))
    try:
        db.execute("""
          INSERT INTO pending_fallbacks(spin_id, user_id, prize, username, created_ts, due_ts, state)
          VALUES (?,?,?,?,?,?, 'pending')
          ON CONFLICT(spin_id) DO NOTHING
        """, (spin_id, user_id, prize, (username or '').lstrip('@'), now, due))
        db.commit()
    except Exception as e:
        print('enqueue_fallback error:', repr(e))

_last_queue_run_ts = None
_last_fallback_run_ts = None

def process_pending_fallbacks(limit: int = 200) -> int:
    global _last_queue_run_ts
    now = utcnow()
    if _last_queue_run_ts and (now - _last_queue_run_ts).total_seconds() < 5:
        return 0
    db = get_db()
    rows = db.execute("""
      SELECT spin_id, user_id, prize, COALESCE(username,'') AS username
      FROM pending_fallbacks
      WHERE state='pending' AND due_ts <= ?
      ORDER BY due_ts ASC
      LIMIT ?
    """, (now, limit)).fetchall()
    processed = 0
    for r in rows:
        spin_id = int(r['spin_id'])
        user_id = int(r['user_id'])
        prize   = r['prize'] or '—'
        uname   = (r['username'] or '').lstrip('@')
        try:
            cur = db.execute("UPDATE pending_fallbacks SET state='sent' WHERE spin_id=? AND state='pending'", (spin_id,))
            if cur.rowcount != 1:
                continue
            exists = db.execute("SELECT 1 FROM lead_events WHERE spin_id=?", (spin_id,)).fetchone()
            if not exists:
                db.execute("INSERT INTO lead_events(spin_id, user_id, type, ts) VALUES (?,?,?,?)",
                           (spin_id, user_id, 'fallback', now))
            db.commit()
        except Exception as e:
            print('process_pending_fallbacks mark error:', repr(e))
            continue
        _send_fallback_message_direct(user_id, spin_id, prize, uname)
        processed += 1
    _last_queue_run_ts = now
    return processed

def process_due_fallbacks(limit: int = 200, grace_seconds: int = FALLBACK_TTL_SECONDS) -> int:
    global _last_fallback_run_ts
    now = utcnow()
    if _last_fallback_run_ts and (now - _last_fallback_run_ts).total_seconds() < 20:
        return 0
    db = get_db()
    due_before = now - timedelta(seconds=grace_seconds)
    rows = db.execute(
        """
        SELECT s.id AS spin_id, s.user_id AS user_id, COALESCE(s.prize,'—') AS prize
        FROM spins s
        LEFT JOIN lead_events le ON le.spin_id = s.id
        WHERE le.spin_id IS NULL AND s.ts <= ?
        ORDER BY s.ts ASC
        LIMIT ?
        """,
        (due_before, limit)
    ).fetchall()
    processed = 0
    for r in rows:
        spin_id = int(r['spin_id'])
        user_id = int(r['user_id'])
        prize   = r['prize']
        try:
            db.execute("INSERT INTO lead_events(spin_id, user_id, type, ts) VALUES (?,?,?,?)",
                       (spin_id, user_id, 'fallback', now))
            db.commit()
        except sqlite3.IntegrityError:
            continue
        _send_fallback_message_direct(user_id, spin_id, prize, '')
        processed += 1
    _last_fallback_run_ts = now
    return processed

def schedule_spin_fallback(spin_id: int, user_id: int, prize: str, username: str):
    enqueue_fallback(spin_id, user_id, prize, username, delay=FALLBACK_TTL_SECONDS)
    def task():
        try:
            with app.app_context():
                process_pending_fallbacks(limit=50)
        except Exception as e:
            print('schedule_spin_fallback timer error:', repr(e))
    t = threading.Timer(FALLBACK_TTL_SECONDS + 5.0, task)
    t.daemon = True
    t.start()

def process_broadcast_queue(limit_per_cycle: int = 15) -> int:
    db = get_db()
    job = db.execute(
        "SELECT * FROM broadcast_jobs WHERE status IN ('pending','running') ORDER BY id ASC LIMIT 1"
    ).fetchone()
    if not job:
        return 0
    job_id = int(job['id'])
    if job['status'] == 'pending':
        db.execute("UPDATE broadcast_jobs SET status='running' WHERE id=?", (job_id,))
        db.commit()
    items = db.execute(
        "SELECT id, user_id FROM broadcast_items WHERE job_id=? AND state='pending' LIMIT ?",
        (job_id, limit_per_cycle)
    ).fetchall()
    if not items:
        pending_left = db.execute(
            "SELECT COUNT(*) AS cnt FROM broadcast_items WHERE job_id=? AND state='pending'",
            (job_id,)
        ).fetchone()['cnt']
        if pending_left == 0 and job['status'] != 'done':
            db.execute("UPDATE broadcast_jobs SET status='done' WHERE id=?", (job_id,))
            db.commit()
        return 0
    text = job['text'] or ''
    parse_mode = (job['parse_mode'] or 'HTML') or None
    if parse_mode not in ('HTML', 'Markdown'):
        parse_mode = None
    attach_ref = bool(job['attach_ref'])
    photo_name = (job['photo_name'] or '').strip()
    sent = skipped = errors = 0
    for row in items:
        item_id = int(row['id'])
        user_id = int(row['user_id'])
        msg = text
        if attach_ref:
            try:
                ref_link = build_ref_link(user_id)
                msg += f"\n\n🔗 Ваша ссылка: {ref_link}"
            except Exception:
                pass
        try:
            if photo_name:
                path = os.path.join(app.static_folder, 'uploads', os.path.basename(photo_name))
                if os.path.exists(path):
                    send_telegram_photo(user_id, path, msg, parse_mode)
                else:
                    send_telegram_message(user_id, msg, parse_mode)
            else:
                send_telegram_message(user_id, msg, parse_mode)
            db.execute(
                "UPDATE broadcast_items SET state='sent', error=NULL WHERE id=?",
                (item_id,)
            )
            sent += 1
        except Forbidden:
            db.execute(
                "UPDATE broadcast_items SET state='skip', error=? WHERE id=?",
                ('forbidden', item_id)
            )
            skipped += 1
        except BadRequest:
            db.execute(
                "UPDATE broadcast_items SET state='skip', error=? WHERE id=?",
                ('bad_request', item_id)
            )
            skipped += 1
        except Exception as e:
            db.execute(
                "UPDATE broadcast_items SET state='error', error=? WHERE id=?",
                (repr(e), item_id)
            )
            errors += 1
        time.sleep(0.1)
    db.execute(
        """UPDATE broadcast_jobs
           SET sent_count    = sent_count    + ?,
               skipped_count = skipped_count + ?,
               error_count   = error_count   + ?
           WHERE id=?""",
        (sent, skipped, errors, job_id)
    )
    db.commit()
    pending_left = db.execute(
        "SELECT COUNT(*) AS cnt FROM broadcast_items WHERE job_id=? AND state='pending'",
        (job_id,)
    ).fetchone()['cnt']
    if pending_left == 0:
        db.execute("UPDATE broadcast_jobs SET status='done' WHERE id=?", (job_id,))
        db.commit()
    return sent + skipped + errors

def is_admin(uid: int) -> bool:
    return uid in ADMIN_USER_IDS

def require_admin(data: dict):
    admin_id = int(data.get('admin_id', 0))
    if not is_admin(admin_id):
        abort(403, description="forbidden")
    return admin_id

# ==== РОУТЫ ====
@app.route('/')
def index():
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
        process_broadcast_queue(limit_per_cycle=5)
    except Exception as e:
        print('process tasks on / error:', repr(e))
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/admin')
def admin_page():
    return send_from_directory(app.static_folder, 'admin.html')

@app.route('/api/check-subscribe', methods=['POST', 'OPTIONS'])
def check_subscribe():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return response
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on check-subscribe error:', repr(e))
    data = request.get_json(force=True)
    user_id = int(data.get('user_id'))
    return jsonify({'subscribed': user_subscribed(user_id)})

@app.route('/api/status', methods=['POST', 'OPTIONS'])
def status():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return response
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on status error:', repr(e))
    data = request.get_json(force=True)
    user_id = int(data.get('user_id'))
    return jsonify(get_status_for(user_id))

@app.route('/api/spin', methods=['POST', 'OPTIONS'])
def spin():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return response
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on spin error:', repr(e))
    data = request.get_json(force=True)
    user_id     = int(data['user_id'])
    username    = (data.get('username') or '').strip().lstrip('@')
    referrer_id = data.get('referrer_id')
    referrer_id = int(referrer_id) if referrer_id is not None else None
    if username:
        dbu = get_db()
        nowu = utcnow()
        try:
            dbu.execute("""
              INSERT INTO audience(user_id, username, added_at)
              VALUES (?,?,?)
              ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, added_at=excluded.added_at
            """, (user_id, username, nowu))
            dbu.commit()
        except Exception:
            pass
    st = get_status_for(user_id)
    if st['attempts_left'] <= 0:
        return jsonify({'error': 'Попыток больше нет.'}), 400
    now = utcnow()
    db = get_db()
    conf = load_wheel_from_db()
    if conf:
        items = [c[0] for c in conf]
        weights = [c[1] for c in conf]
    else:
        items = PRIZES[:]
        weights = PRIZE_WEIGHTS[:]
    prize = weighted_choice(items, weights)
    cur = db.execute("INSERT INTO spins(user_id, ts, prize) VALUES (?,?,?)", (user_id, now, prize))
    spin_id = cur.lastrowid
    db.commit()
    schedule_spin_fallback(spin_id, user_id, prize, username)
    if referrer_id and referrer_id != user_id:
        try:
            db.execute(
                "INSERT INTO referrals(referrer_id, referred_id, ts) VALUES (?,?,?)",
                (referrer_id, user_id, now)
            )
            db.commit()
        except sqlite3.IntegrityError:
            pass
    return jsonify({'prize': prize, 'spin_id': spin_id})

@app.route('/api/submit-lead', methods=['POST', 'OPTIONS'])
def submit_lead():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return response
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on submit-lead error:', repr(e))
    data = request.get_json(force=True)
    user_id   = int(data['user_id'])
    spin_id   = int(data['spin_id'])
    name      = (data.get('name') or '').strip()
    phone     = (data.get('phone') or '').strip()
    username  = (data.get('username') or '').strip().lstrip('@')
    db = get_db()
    if username:
        nowu = utcnow()
        try:
            db.execute("""
              INSERT INTO audience(user_id, username, added_at)
              VALUES (?,?,?)
              ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, added_at=excluded.added_at
            """, (user_id, username, nowu))
            db.commit()
        except Exception:
            pass
    row = db.execute("SELECT prize FROM spins WHERE id=? AND user_id=?", (spin_id, user_id)).fetchone()
    prize = row['prize'] if row and row['prize'] else '—'
    now = utcnow()
    db.execute(
        "INSERT INTO leads(user_id, username, name, phone, ts) VALUES (?,?,?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, name=excluded.name, phone=excluded.phone, ts=excluded.ts",
        (user_id, username, name, phone, now)
    )
    existed = db.execute("SELECT 1 FROM lead_events WHERE spin_id=?", (spin_id,)).fetchone()
    if not existed:
        db.execute("INSERT INTO lead_events(spin_id, user_id, type, ts) VALUES (?,?,?,?)",
                   (spin_id, user_id, 'full', now))
    db.execute("UPDATE pending_fallbacks SET state='full' WHERE spin_id=?", (spin_id,))
    db.commit()
    text = (
        f"<b>📥 Лид (полный)</b>\n"
        f"SpinID: <code>{spin_id}</code>\n"
        f"UserID: <code>{user_id}</code>\n"
        f"Username: @{escape(username) if username else '—'}\n"
        f"Имя: {escape(name)}\n"
        f"Телефон: {escape(phone) if phone else '—'}\n"
        f"Результат: {escape(prize)}"
    )
    send_telegram_message(chat_id=LEADS_TARGET_ID, text=text, parse_mode='HTML')
    return jsonify({'ok': True})

@app.route('/api/lead-fallback', methods=['POST', 'OPTIONS'])
def lead_fallback():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST,OPTIONS')
        return response
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on lead-fallback error:', repr(e))
    data = request.get_json(force=True)
    user_id  = int(data['user_id'])
    spin_id  = int(data['spin_id'])
    username = (data.get('username') or '').strip().lstrip('@')
    name     = (data.get('name') or '').strip()
    db = get_db()
    ev = db.execute("SELECT type FROM lead_events WHERE spin_id=?", (spin_id,)).fetchone()
    if ev and ev['type'] == 'full':
        return jsonify({'ok': True, 'skipped': True})
    cur = db.execute("UPDATE pending_fallbacks SET state='sent' WHERE spin_id=? AND state='pending'", (spin_id,))
    now = utcnow()
    row = db.execute("SELECT prize FROM spins WHERE id=? AND user_id=?", (spin_id, user_id)).fetchone()
    prize = row['prize'] if row and row['prize'] else '—'
    existed = db.execute("SELECT 1 FROM lead_events WHERE spin_id=?", (spin_id,)).fetchone()
    if not existed:
        db.execute("INSERT INTO lead_events(spin_id, user_id, type, ts) VALUES (?,?,?,?)",
                   (spin_id, user_id, 'fallback', now))
    db.commit()
    text = (
        f"<b>📥 Лид (без телефона)</b>\n"
        f"SpinID: <code>{spin_id}</code>\n"
        f"UserID: <code>{user_id}</code>\n"
        f"Username: @{escape(username) if username else '—'}\n"
        f"Имя: {escape(name) if name else '—'}\n"
        f"Телефон: —\n"
        f"Результат: {escape(prize)}"
    )
    send_telegram_message(chat_id=LEADS_TARGET_ID, text=text, parse_mode='HTML')
    return jsonify({'ok': True})

@app.route('/api/process-fallbacks', methods=['POST', 'GET', 'OPTIONS'])
def process_fallbacks_endpoint():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'POST,GET,OPTIONS')
        return response
    try:
        c1 = process_pending_fallbacks(limit=300)
        c2 = process_due_fallbacks(limit=200, grace_seconds=FALLBACK_TTL_SECONDS)
        return jsonify({'processed_from_queue': c1, 'processed_legacy': c2, 'total': (c1+c2)})
    except Exception as e:
        return jsonify({'error': repr(e)}), 500

@app.route('/api/wheel-config', methods=['GET', 'OPTIONS'])
def get_wheel_config():
    if request.method == 'OPTIONS':
        response = make_response()
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Requested-With')
        response.headers.add('Access-Control-Allow-Methods', 'GET,OPTIONS')
        return response
    conf = load_wheel_from_db()
    if conf:
        items = [{'label': c[0], 'weight': int(c[1]), 'win_text': c[2]} for c in conf]
    else:
        items = [{'label': l, 'weight': w, 'win_text': ''} for l, w in zip(PRIZES, PRIZE_WEIGHTS)]
    return jsonify({'items': items})

@app.route('/health')
def health():
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
        process_broadcast_queue(limit_per_cycle=5)
    except Exception as e:
        print('process tasks on /health error:', repr(e))
    return 'ok'

def _sweeper_loop():
    while True:
        try:
            with app.app_context():
                try:
                    process_pending_fallbacks(limit=300)
                    process_due_fallbacks(limit=200, grace_seconds=FALLBACK_TTL_SECONDS)
                except Exception as e:
                    print('sweeper fallbacks error:', repr(e))
                try:
                    process_broadcast_queue(limit_per_cycle=15)
                except Exception as e:
                    print('sweeper process_broadcast_queue error:', repr(e))
        except Exception as e:
            print('sweeper outer error:', repr(e))
        time.sleep(max(5, SWEEP_INTERVAL_SECONDS))

def _start_sweeper_once():
    if getattr(_start_sweeper_once, '_started', False):
        return
    _start_sweeper_once._started = True
    t = threading.Thread(target=_sweeper_loop, daemon=True)
    t.start()

_start_sweeper_once()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
