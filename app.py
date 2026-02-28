import os
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from html import escape

from flask import Flask, request, jsonify, g, send_from_directory, abort
import telegram
import threading
import time  # нужен и для таймеров, и для троттлинга в админке
import re

# ==== ПЕРЕМЕННЫЕ (Студия BODYFACEROOM) ====
BOT_TOKEN  = os.environ.get('BOT_TOKEN', '8275038606:AAEubsCRXwU4xnQfolCTr0jCwHqH7ZRxgxA')
SUBSCRIPTION_CHANNEL_ID = os.environ.get('SUBSCRIPTION_CHANNEL_ID', '-1001698393800')
LEADS_TARGET_ID         = os.environ.get('LEADS_TARGET_ID', '-1003413060996')
BOT_USERNAME = os.environ.get('BOT_USERNAME', 'BODYFACEROOMbot').strip('@')

# Админы (главный + доп.админы для админки)
_admin_env = os.environ.get(
    'ADMIN_USER_IDS',
    '291655689,463942971,5230371449'
).replace(' ', '')
ADMIN_USER_IDS = [int(x) for x in _admin_env.split(',') if x]

TZ = timezone.utc

# Лимиты
# BASE_ATTEMPTS_PER_DAY трактуем как "базовый запас попыток" (2), а не "в день"
BASE_ATTEMPTS_PER_DAY = int(os.environ.get('BASE_ATTEMPTS_PER_DAY', '2'))
# REFERRAL_BONUS — +1 попытка за каждого приглашённого по рефералке
REFERRAL_BONUS        = int(os.environ.get('REFERRAL_BONUS', '1'))

# Периоды
SWEEP_INTERVAL_SECONDS = int(os.environ.get('SWEEP_INTERVAL_SECONDS', '60'))
FALLBACK_TTL_SECONDS   = int(os.environ.get('FALLBACK_TTL_SECONDS', '120'))

# ==== БАЗОВЫЕ ПРИЗЫ BODYFACEROOM (фолбэк, если нет конфигурации в БД) ====
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
# Все призы с одинаковым весом (всё равно основной конфиг берётся из админки)
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

bot = telegram.Bot(token=BOT_TOKEN)
app = Flask(__name__, static_folder='static', static_url_path='')

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
      type      TEXT NOT NULL,  -- 'full' or 'fallback'
      ts        TIMESTAMP NOT NULL
    );
    CREATE TABLE IF NOT EXISTS audience (
      user_id   INTEGER PRIMARY KEY,
      username  TEXT,
      added_at  TIMESTAMP NOT NULL
    );

    /* Конфиг колеса (редактируется через админку) */
    CREATE TABLE IF NOT EXISTS wheel_items (
      id       INTEGER PRIMARY KEY,
      pos      INTEGER NOT NULL,
      label    TEXT NOT NULL,
      win_text TEXT,
      weight   INTEGER NOT NULL DEFAULT 0
    );

    /* Очередь фолбэков: гарантирует доставку даже если пользователь закрыл окно */
    CREATE TABLE IF NOT EXISTS pending_fallbacks (
      spin_id    INTEGER PRIMARY KEY,
      user_id    INTEGER NOT NULL,
      prize      TEXT,
      username   TEXT,
      created_ts TIMESTAMP NOT NULL,
      due_ts     TIMESTAMP NOT NULL,
      state      TEXT NOT NULL DEFAULT 'pending'  -- pending|sent|full
    );

    /* Очередь рассылок для админки */
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

def start_of_day(dt):
    return datetime(dt.year, dt.month, dt.day, tzinfo=TZ)

def user_subscribed(user_id: int) -> bool:
    try:
        member = bot.get_chat_member(chat_id=SUBSCRIPTION_CHANNEL_ID, user_id=user_id)
        return member.status not in ('left', 'kicked')
    except Exception as e:
        print("check_subscribe error:", repr(e))
        return False

def build_ref_link(user_id: int) -> str:
    code = f"uid_{user_id}"
    return f"https://t.me/{BOT_USERNAME}?startapp={code}"

def get_status_for(user_id: int):
    """
    Логика попыток (lifetime):
    - 2 базовые попытки (BASE_ATTEMPTS_PER_DAY трактуем как "базовый запас");
    - +REFERRAL_BONUS попыток за КАЖДОГО приглашённого по рефералке (навсегда);
    - попытки не обновляются по дням: считаем по жизни.
    """
    db = get_db()

    # Все спины пользователя за всё время
    total_spins = db.execute(
        "SELECT COUNT(*) AS cnt FROM spins WHERE user_id=?",
        (user_id,)
    ).fetchone()['cnt']

    # Все рефералы за всё время
    total_referrals = db.execute(
        "SELECT COUNT(*) AS cnt FROM referrals WHERE referrer_id=?",
        (user_id,)
    ).fetchone()['cnt']

    # Базовый запас + бонусы за рефералов
    base_attempts = BASE_ATTEMPTS_PER_DAY
    attempts_granted = base_attempts + REFERRAL_BONUS * total_referrals

    attempts_left = max(0, attempts_granted - total_spins)
    ref_link = build_ref_link(user_id)

    return {
        'attempts_left': attempts_left,
        'bonus': total_referrals,   # количество рефералов всего
        'spins_today': total_spins, # здесь можно использовать "всего спинов"
        'ref_link': ref_link
    }

# ===== Конфиг колеса =====
def load_wheel_from_db():
    db = get_db()
    rows = db.execute("SELECT label, weight, win_text FROM wheel_items ORDER BY pos ASC").fetchall()
    if not rows:
        return None
    return [(r['label'], int(r['weight'] or 0), r['win_text'] or '') for r in rows]

# ===== Очередь фолбэков + свипер =====
_last_fallback_run_ts = None
_last_queue_run_ts = None

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
    try:
        bot.send_message(chat_id=LEADS_TARGET_ID, text=text, parse_mode='HTML')
    except Exception as e:
        print('send fallback message error:', repr(e))

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

# ===== ОЧЕРЕДЬ РАССЫЛОК (для больших отправок из админки) =====
def process_broadcast_queue(limit_per_cycle: int = 15) -> int:
    """Обрабатывает очередь рассылок, не блокируя веб-запросы."""
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
                    with open(path, 'rb') as f:
                        bot.send_photo(chat_id=user_id, photo=f, caption=msg, parse_mode=parse_mode)
                else:
                    bot.send_message(chat_id=user_id, text=msg, parse_mode=parse_mode)
            else:
                bot.send_message(chat_id=user_id, text=msg, parse_mode=parse_mode)
            db.execute(
                "UPDATE broadcast_items SET state='sent', error=NULL WHERE id=?",
                (item_id,)
            )
            sent += 1
        except telegram.error.Forbidden:
            db.execute(
                "UPDATE broadcast_items SET state='skip', error=? WHERE id=?",
                ('forbidden', item_id)
            )
            skipped += 1
        except telegram.error.BadRequest:
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

# ===== Админ-помощники =====
def is_admin(uid: int) -> bool:
    return uid in ADMIN_USER_IDS

def require_admin(data: dict):
    admin_id = int(data.get('admin_id', 0))
    if not is_admin(admin_id):
        abort(403, description="forbidden")
    return admin_id

# ===== РОУТЫ =====
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

@app.route('/api/check-subscribe', methods=['POST'])
def check_subscribe():
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on check-subscribe error:', repr(e))
    data = request.get_json(force=True)
    user_id = int(data.get('user_id'))
    return jsonify({'subscribed': user_subscribed(user_id)})

@app.route('/api/status', methods=['POST'])
def status():
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
    except Exception as e:
        print('process fallbacks on status error:', repr(e))
    data = request.get_json(force=True)
    user_id = int(data.get('user_id'))
    return jsonify(get_status_for(user_id))

@app.route('/api/spin', methods=['POST'])
def spin():
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

    # апсерт username в audience (если пришёл)
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
        # больше не привязываем к "сегодня" — попыток просто нет
        return jsonify({'error': 'Попыток больше нет.'}), 400

    now = utcnow()
    db = get_db()

    # конфиг из БД или фолбэк
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

    # непробиваемый фолбэк
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

@app.route('/api/submit-lead', methods=['POST'])
def submit_lead():
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

    # апсерт username в audience
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
    # гасим возможный фолбэк из очереди
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
    try:
        bot.send_message(chat_id=LEADS_TARGET_ID, text=text, parse_mode='HTML')
    except Exception as e:
        print('send lead full error:', repr(e))

    return jsonify({'ok': True})

@app.route('/api/lead-fallback', methods=['POST'])
def lead_fallback():
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
    try:
        bot.send_message(chat_id=LEADS_TARGET_ID, text=text, parse_mode='HTML')
    except Exception as e:
        print('send lead fallback error:', repr(e))

    return jsonify({'ok': True})

@app.route('/api/process-fallbacks', methods=['POST', 'GET'])
def process_fallbacks_endpoint():
    try:
        c1 = process_pending_fallbacks(limit=300)
        c2 = process_due_fallbacks(limit=200, grace_seconds=FALLBACK_TTL_SECONDS)
        return jsonify({'processed_from_queue': c1, 'processed_legacy': c2, 'total': (c1+c2)})
    except Exception as e:
        return jsonify({'error': repr(e)}), 500

# ===== API АДМИНКИ =====
@app.route('/api/admin/recipients', methods=['POST'])
def admin_recipients():
    try:
        process_due_fallbacks()
    except Exception as e:
        print('process_due_fallbacks on admin_recipients:', repr(e))

    data = request.get_json(force=True)
    require_admin(data)

    since_days  = int(data.get('since_days') or 0)
    limit       = max(1, min(int(data.get('limit') or 200), 1000))
    offset      = max(0, int(data.get('offset') or 0))
    check_sub   = bool(data.get('check_sub') or False)

    db = get_db()
    sql = """
    WITH seen AS (
      SELECT user_id AS uid, MAX(ts) AS last_ts FROM spins GROUP BY user_id
      UNION ALL
      SELECT user_id AS uid, MAX(ts) AS last_ts FROM leads GROUP BY user_id
      UNION ALL
      SELECT referrer_id AS uid, MAX(ts) AS last_ts FROM referrals GROUP BY referrer_id
      UNION ALL
      SELECT referred_id AS uid, MAX(ts) AS last_ts FROM referrals GROUP BY referred_id
      UNION ALL
      SELECT user_id AS uid, MAX(added_at) AS last_ts FROM audience GROUP BY user_id
    ),
    agg AS ( SELECT uid, MAX(last_ts) AS last_seen FROM seen GROUP BY uid )
    SELECT uid, last_seen FROM agg
    """
    params = []
    if since_days > 0:
        sql += " WHERE last_seen >= ?"
        params.append(utcnow() - timedelta(days=since_days))
    sql += " ORDER BY last_seen DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = db.execute(sql, params).fetchall()
    users = []
    for r in rows:
        uid = int(r['uid']) if r['uid'] is not None else None
        if not uid:
            continue
        item = {
            'user_id': uid,
            'last_seen': (r['last_seen'].isoformat() if isinstance(r['last_seen'], datetime) else str(r['last_seen'])),
            'username': None,
            'subscribed': None,
        }
        row_u = db.execute("SELECT username FROM leads WHERE user_id=? ORDER BY ts DESC LIMIT 1", (uid,)).fetchone()
        if row_u and row_u['username']:
            item['username'] = row_u['username'].lstrip('@')
        else:
            row_a = db.execute("SELECT username FROM audience WHERE user_id=? LIMIT 1", (uid,)).fetchone()
            if row_a and row_a['username']:
                item['username'] = row_a['username'].lstrip('@')
        users.append(item)

    if check_sub:
        for it in users:
            try:
                m = bot.get_chat_member(chat_id=SUBSCRIPTION_CHANNEL_ID, user_id=it['user_id'])
                it['subscribed'] = (m.status not in ('left','kicked'))
            except Exception:
                it['subscribed'] = None

    return jsonify({'items': users, 'count': len(users)})

@app.route('/api/admin/send-chunk', methods=['POST'])
def admin_send_chunk():
    # ОСТАВЛЕНО ДЛЯ СОВМЕСТИМОСТИ, но лучше пользоваться очередью broadcast
    data = request.get_json(force=True)
    require_admin(data)

    ids = data.get('ids') or []
    if not isinstance(ids, list) or not ids:
        return jsonify({'error': 'ids required'}), 400
    ids = [int(x) for x in ids][:30]

    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({'error': 'text required'}), 400

    parse_mode = data.get('parse_mode') or 'HTML'
    if parse_mode not in ('HTML','Markdown','None'):
        parse_mode = 'HTML'
    parse_mode = (None if parse_mode == 'None' else parse_mode)

    attach_ref = bool(data.get('attach_ref') or False)
    photo_name = (data.get('photo') or '').strip()

    sent = 0; skipped = 0; errors = 0
    results = []
    for uid in ids:
        msg = text
        if attach_ref:
            msg += f"\n\n🔗 Ваша ссылка: https://t.me/{BOT_USERNAME}?startapp=uid_{uid}"
        try:
            if photo_name:
                path = os.path.join(app.static_folder, 'uploads', os.path.basename(photo_name))
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        bot.send_photo(chat_id=uid, photo=f, caption=msg, parse_mode=parse_mode)
                else:
                    bot.send_message(chat_id=uid, text=msg, parse_mode=parse_mode)
            else:
                bot.send_message(chat_id=uid, text=msg, parse_mode=parse_mode)
            sent += 1
            results.append({'user_id': uid, 'status': 'sent'})
            time.sleep(0.07)
        except telegram.error.Forbidden:
            skipped += 1
            results.append({'user_id': uid, 'status': 'skip', 'note': 'forbidden'})
        except telegram.error.BadRequest:
            skipped += 1
            results.append({'user_id': uid, 'status': 'skip', 'note': 'bad_request'})
        except Exception as e:
            errors += 1
            results.append({'user_id': uid, 'status': 'error', 'note': repr(e)})
    return jsonify({'sent': sent, 'skipped': skipped, 'errors': errors, 'results': results})

@app.route('/api/admin/broadcast-create', methods=['POST'])
def admin_broadcast_create():
    data = request.get_json(force=True)
    admin_id = require_admin(data)

    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({'error': 'text required'}), 400

    parse_mode = data.get('parse_mode') or 'HTML'
    if parse_mode not in ('HTML', 'Markdown', 'None'):
        parse_mode = 'HTML'
    parse_mode = None if parse_mode == 'None' else parse_mode

    attach_ref = bool(data.get('attach_ref') or False)
    photo_name = (data.get('photo') or '').strip()

    ids = data.get('ids') or []
    if not isinstance(ids, list) or not ids:
        return jsonify({'error': 'ids required'}), 400

    try:
        uniq_ids = sorted({int(x) for x in ids if int(x) > 0})
    except Exception:
        return jsonify({'error': 'bad ids'}), 400

    total = len(uniq_ids)
    now = utcnow()
    db = get_db()
    with db:
        cur = db.execute(
            "INSERT INTO broadcast_jobs("
            "created_at, created_by, text, parse_mode, attach_ref, photo_name, total_recipients, status"
            ") VALUES (?,?,?,?,?,?,?, 'pending')",
            (now, admin_id, text, parse_mode, 1 if attach_ref else 0, photo_name, total)
        )
        job_id = cur.lastrowid
        for uid in uniq_ids:
            db.execute(
                "INSERT INTO broadcast_items(job_id, user_id, state) VALUES (?,?, 'pending')",
                (job_id, uid)
            )
    return jsonify({'ok': True, 'job_id': job_id, 'total': total})

@app.route('/api/admin/broadcast-status', methods=['POST'])
def admin_broadcast_status():
    data = request.get_json(force=True)
    require_admin(data)
    job_id = int(data.get('job_id') or 0)
    if not job_id:
        return jsonify({'error': 'job_id required'}), 400

    db = get_db()
    job = db.execute(
        "SELECT id, created_at, created_by, text, parse_mode, attach_ref, photo_name, "
        "total_recipients, sent_count, skipped_count, error_count, status "
        "FROM broadcast_jobs WHERE id=?",
        (job_id,)
    ).fetchone()
    if not job:
        return jsonify({'error': 'not_found'}), 404

    pending = db.execute(
        "SELECT COUNT(*) AS cnt FROM broadcast_items WHERE job_id=? AND state='pending'",
        (job_id,)
    ).fetchone()['cnt']

    created_at = job['created_at']
    if isinstance(created_at, datetime):
        created_at_str = created_at.isoformat()
    else:
        created_at_str = str(created_at)

    return jsonify({
        'job': {
            'id': job['id'],
            'created_at': created_at_str,
            'created_by': job['created_by'],
            'parse_mode': job['parse_mode'],
            'attach_ref': bool(job['attach_ref']),
            'photo_name': job['photo_name'],
            'total_recipients': job['total_recipients'],
            'sent_count': job['sent_count'],
            'skipped_count': job['skipped_count'],
            'error_count': job['error_count'],
            'status': job['status'],
        },
        'pending': pending
    })

@app.route('/api/admin/import', methods=['POST'])
def admin_import():
    data = request.get_json(force=True)
    require_admin(data)

    raw = (data.get('text') or '').strip()
    if not raw:
        return jsonify({'error': 'empty'}), 400

    pairs = re.findall(r"User ?ID:\s*(\d+)[\s\S]{0,250}?Username:\s*@?([^\s\n]+)", raw, flags=re.IGNORECASE)
    ids   = re.findall(r"User ?ID:\s*([0-9]+)", raw, flags=re.IGNORECASE)
    names = re.findall(r"Username:\s*@?([^\s\n]+)", raw, flags=re.IGNORECASE)

    stitched = []
    m = min(len(ids), len(names))
    for i in range(m):
        stitched.append((ids[i], names[i]))

    result = {}
    for uid, uname in pairs + stitched:
        uid_i = int(uid)
        uname_norm = (uname or '').strip()
        if uname_norm in ('—', '-', '—', '—'):  # на всякий
            uname_norm = ''
        uname_norm = uname_norm.lstrip('@')
        if uid_i not in result or (uname_norm and not result[uid_i]):
            result[uid_i] = uname_norm

    for uid in ids:
        uid_i = int(uid)
        result.setdefault(uid_i, "")

    if not result:
        return jsonify({'error': 'no_ids_found'}), 400

    db = get_db()
    now = utcnow()
    processed = 0
    for uid, uname in result.items():
        if uname:
            db.execute("""
              INSERT INTO audience(user_id, username, added_at)
              VALUES (?,?,?)
              ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, added_at=excluded.added_at
            """, (uid, uname, now))
        else:
            db.execute("""
              INSERT INTO audience(user_id, username, added_at)
              VALUES (?,NULL,?)
              ON CONFLICT(user_id) DO UPDATE SET added_at=excluded.added_at
            """, (uid, now))
        processed += 1
    db.commit()
    return jsonify({'ok': True, 'processed': processed})

@app.route('/api/admin/upload', methods=['POST'])
def admin_upload_photo():
    admin_id = int((request.form.get('admin_id') or 0))
    if not is_admin(admin_id):
        abort(403, description="forbidden")

    if 'photo' not in request.files:
        return jsonify({'error': 'no_file'}), 400
    file = request.files['photo']
    if not file.filename:
        return jsonify({'error': 'empty_name'}), 400

    os.makedirs(os.path.join(app.static_folder, 'uploads'), exist_ok=True)
    ext = os.path.splitext(file.filename)[1].lower()[:10]
    safe_ext = ext if ext in ('.jpg','.jpeg','.png','.gif','.webp') else '.jpg'
    fname = f"{int(time.time())}_{random.randint(1000,9999)}{safe_ext}"
    save_path = os.path.join(app.static_folder, 'uploads', fname)
    file.save(save_path)

    return jsonify({'ok': True, 'name': fname, 'url': f"/uploads/{fname}"})

@app.route('/api/wheel-config', methods=['GET'])
def get_wheel_config():
    conf = load_wheel_from_db()
    if conf:
        items = [{'label': c[0], 'weight': int(c[1]), 'win_text': c[2]} for c in conf]
    else:
        items = [{'label': l, 'weight': w, 'win_text': ''} for l, w in zip(PRIZES, PRIZE_WEIGHTS)]
    return jsonify({'items': items})

@app.route('/api/admin/wheel-config', methods=['POST'])
def save_wheel_config():
    data = request.get_json(force=True)
    require_admin(data)
    items = data.get('items') or []
    if not isinstance(items, list):
        return jsonify({'error': 'items must be list'}), 400

    db = get_db()
    with db:
        db.execute("DELETE FROM wheel_items;")
        for i, it in enumerate(items):
            label = (it.get('label') or '').strip()
            win_text = (it.get('win_text') or '').strip()
            weight = int(it.get('weight') or 0)
            if not label:
                continue
            db.execute(
                "INSERT INTO wheel_items(pos, label, win_text, weight) VALUES (?,?,?,?)",
                (i, label, win_text, max(0, weight))
            )
    return jsonify({'ok': True, 'count': len(items)})

@app.route('/api/admin/reset-attempts', methods=['POST'])
def reset_attempts_all():
    data = request.get_json(force=True)
    require_admin(data)
    since_days = int(data.get('since_days') or 0)
    now = utcnow()

    db = get_db()
    sql_seen = """
    WITH seen AS (
      SELECT user_id AS uid, MAX(ts) AS last_ts FROM spins GROUP BY user_id
      UNION ALL
      SELECT user_id AS uid, MAX(ts) AS last_ts FROM leads GROUP BY user_id
      UNION ALL
      SELECT referrer_id AS uid, MAX(ts) AS last_ts FROM referrals GROUP BY referrer_id
      UNION ALL
      SELECT referred_id AS uid, MAX(ts) AS last_ts FROM referrals GROUP BY referred_id
      UNION ALL
      SELECT user_id AS uid, MAX(added_at) AS last_ts FROM audience GROUP BY user_id
    ),
    agg AS ( SELECT uid, MAX(last_ts) AS last_seen FROM seen GROUP BY uid )
    SELECT uid FROM agg {where_clause}
    """
    params = []
    where_clause = ""
    if since_days > 0:
        where_clause = "WHERE last_seen >= ?"
        params.append(now - timedelta(days=since_days))
    rows = db.execute(sql_seen.format(where_clause=where_clause), params).fetchall()
    uids = [int(r['uid']) for r in rows if r['uid'] is not None]
    if not uids:
        return jsonify({'reset': 0})
    reset = 0
    with db:
        for chunk_pos in range(0, len(uids), 500):
            chunk = uids[chunk_pos:chunk_pos+500]
            qmarks = ",".join("?"*len(chunk))
            # Обнуляем ВСЕ спины по пользователям (lifetime-reset)
            reset += db.execute(
                f"DELETE FROM spins WHERE user_id IN ({qmarks})",
                chunk
            ).rowcount
    return jsonify({'reset': reset})

@app.route('/api/admin/reset-attempts-selected', methods=['POST'])
def reset_attempts_selected():
    data = request.get_json(force=True)
    require_admin(data)
    ids = data.get('ids') or []
    if not isinstance(ids, list) or not ids:
        return jsonify({'error': 'ids required'}), 400
    ids = [int(x) for x in ids]
    db = get_db()
    reset = 0
    with db:
        for chunk_pos in range(0, len(ids), 500):
            chunk = ids[chunk_pos:chunk_pos+500]
            qmarks = ",".join("?"*len(chunk))
            # Обнуляем ВСЕ спины выбранных пользователей
            reset += db.execute(
                f"DELETE FROM spins WHERE user_id IN ({qmarks})",
                chunk
            ).rowcount
    return jsonify({'reset': reset})

@app.route('/health')
def health():
    try:
        process_pending_fallbacks()
        process_due_fallbacks()
        process_broadcast_queue(limit_per_cycle=5)
    except Exception as e:
        print('process tasks on /health error:', repr(e))
    return 'ok'

# ==== ФОНОВЫЙ СВИПЕР (фолбэки + рассылки) ====
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

