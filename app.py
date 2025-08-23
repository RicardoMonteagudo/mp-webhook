from flask import Flask, request
import os, requests, json, time
from psycopg2.pool import SimpleConnectionPool
import psycopg2.extras

app = Flask(__name__)

MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")  # opcional

# ===== DB envs =====
DB_HOST   = os.getenv("DB_HOST")
DB_NAME   = os.getenv("DB_NAME", "core_prod")
DB_USER   = os.getenv("DB_USER", "baikarool")
DB_PASS   = os.getenv("DB_PASS")
DB_SCHEMA = os.getenv("DB_SCHEMA", "baikarool")

POOL = None

def ensure_pool():
    """Crea el pool si hay credenciales; no truena si faltan."""
    global POOL
    if POOL is not None:
        return
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
        print("⚠️ DB no configurada (faltan envs). Sigo sin DB.", flush=True)
        return
    try:
        POOL = SimpleConnectionPool(
            minconn=1, maxconn=3,
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS,
            connect_timeout=10, options=f"-c search_path={DB_SCHEMA},public"
        )
        print("✅ Pool a Postgres listo", flush=True)
    except Exception as e:
        print("❌ Error creando pool:", e, flush=True)

def _to_bigint_or_none(val):
    if val is None:
        return None
    try:
        # Mercadopago manda ids como int o str; solo convierte si es numérico
        return int(str(val))
    except Exception:
        return None

def save_webhook_to_events(payload: dict):
    """Inserta en {schema}.webhook_events con tu esquema de columnas."""
    ensure_pool()
    if POOL is None:
        return

    topic = payload.get("topic") or "unknown"
    # event_id: el id del evento si viene; si no, fabricamos uno legible
    event_id = (
        payload.get("id")
        or payload.get("event_id")
        or (payload.get("data") or {}).get("id")
        or f"{topic}-{int(time.time()*1000)}"
    )
    # payment_id: sólo si existe y es numérico (bigint)
    payment_id = (
        _to_bigint_or_none(payload.get("payment_id"))
        or _to_bigint_or_none(payload.get("id"))
        or _to_bigint_or_none((payload.get("data") or {}).get("id"))
    )

    conn = None
    try:
        conn = POOL.getconn()
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                INSERT INTO {DB_SCHEMA}.webhook_events
                   (event_id, topic, payment_id, received_at, attempt, raw_payload)
                VALUES
                   (%s,       %s,    %s,         now(),      %s,      %s::jsonb)
                """,
                (str(event_id), topic, payment_id, 1, json.dumps(payload))
            )
        print("💾 Guardado en webhook_events", flush=True)
    except Exception as e:
        print("❌ Error guardando webhook:", e, flush=True)
    finally:
        if conn:
            POOL.putconn(conn)

@app.route("/webhook", methods=["POST"])
def webhook():
    # 1) Armar un payload unificado sin forzar tipo de contenido
    payload = {}
    if request.is_json:
        payload.update(request.get_json(silent=True) or {})
    payload.update(request.form.to_dict())
    payload.update(request.args.to_dict())

    print("📩 Headers:", dict(request.headers), flush=True)
    print("📦 Payload:", payload, flush=True)

    # 👉 Insert compatible con tu tabla
    save_webhook_to_events(payload)

    # 2) (Opcional) Si vino un pago, consultar detalles a la API
    topic = payload.get("topic")
    resource_id = payload.get("id") or payload.get("resource_id")
    if topic == "payment" and resource_id and MP_ACCESS_TOKEN:
        try:
            r = requests.get(
                f"https://api.mercadopago.com/v1/payments/{resource_id}",
                headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"},
                timeout=10,
            )
            print("🔎 MP payment:", r.json(), flush=True)
        except Exception as e:
            print("⚠️ Error consultando MP:", e, flush=True)

    # 3) Siempre devolver 200 lo más rápido posible
    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200
