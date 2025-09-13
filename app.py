from flask import Flask, request
import os, json, time
import psycopg2
import psycopg2.extras
import requests
from decimal import Decimal

app = Flask(__name__)

# ========= ENV =========
INSTANCE   = os.environ["INSTANCE_CONNECTION_NAME"]
DB_NAME    = os.environ["DB_NAME"]
DB_USER    = os.environ["DB_USER"]
DB_PASS    = os.environ["DB_PASS"]
DB_SCHEMA  = os.getenv("DB_SCHEMA", "baikarool")
MP_TOKEN   = os.getenv("MP_ACCESS_TOKEN")  # opcional, pero necesario para /v1/payments/{id}

# ========= DB =========
def get_conn():
    """Conexión a Cloud SQL (Postgres) por socket unix."""
    return psycopg2.connect(
        host=f"/cloudsql/{INSTANCE}",
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=5432,
        options=f"-c search_path={DB_SCHEMA},public"
    )

def psy_now():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT now()")
        return cur.fetchone()[0]

def get_table_columns(table: str) -> set:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (DB_SCHEMA, table))
        return {r[0] for r in cur.fetchall()}

def upsert_row(table: str, pk_cols: list[str], row: dict):
    """
    UPSERT seguro: solo columnas existentes y con valor.
    Evita sobreescribir con NULL y no truena si faltan columnas.
    """
    cols_in_db = get_table_columns(table)
    data = {k: v for k, v in row.items() if v is not None and k in cols_in_db}
    if not data:
        print(f"⚠️ Nada que upsert en {table}", flush=True)
        return False

    cols = list(data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(cols)
    pk_list = ", ".join(pk_cols)

    update_cols = [c for c in cols if c not in pk_cols]
    if update_cols:
        setlist = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
        sql = f"""
        INSERT INTO {DB_SCHEMA}.{table} ({collist})
        VALUES ({placeholders})
        ON CONFLICT ({pk_list}) DO UPDATE SET {setlist}
        """
    else:
        sql = f"""
        INSERT INTO {DB_SCHEMA}.{table} ({collist})
        VALUES ({placeholders})
        ON CONFLICT ({pk_list}) DO NOTHING
        """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [data[c] for c in cols])
    return True

def update_row_fields(table: str, where_k: str, where_v, fields: dict):
    cols_in_db = get_table_columns(table)
    data = {k: v for k, v in fields.items() if k in cols_in_db}
    if not data:
        return
    setlist = ", ".join([f"{k}=%s" for k in data.keys()])
    sql = f"UPDATE {DB_SCHEMA}.{table} SET {setlist} WHERE {where_k}=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, list(data.values()) + [where_v])

# ========= Helpers =========
def _to_bigint_or_none(val):
    if val is None: return None
    try: return int(str(val))
    except Exception: return None

def _to_decimal_or_none(val):
    if val is None: return None
    try: return Decimal(str(val))
    except Exception: return None

def unify_payload(req) -> dict:
    payload = {}
    if req.is_json:
        payload.update(req.get_json(silent=True) or {})
    payload.update(req.form.to_dict())
    payload.update(req.args.to_dict())
    return payload

# ========= Mercado Pago =========
def get_payment_from_mp(payment_id: int) -> dict | None:
    if not (MP_TOKEN and payment_id):
        return None
    url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {MP_TOKEN}"}, timeout=10)
        if r.status_code == 200:
            return r.json()
        print(f"⚠️ MP /v1/payments/{payment_id} status={r.status_code} body={r.text[:300]}", flush=True)
    except Exception as e:
        print("⚠️ Error consultando MP:", e, flush=True)
    return None

# ========= Mapeos =========
def map_payments_row(mp: dict) -> dict:
    # Mapea SOLO a columnas que existen en tu tabla (upsert filtra de todas formas)
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "status": (mp.get("status") or "").lower() or None,
        "status_detail": mp.get("status_detail"),
        "amount": _to_decimal_or_none(mp.get("transaction_amount")),
        "currency": mp.get("currency_id"),
        "date_created": mp.get("date_created"),
        "date_approved": mp.get("date_approved"),
        "payment_method_id": mp.get("payment_method_id"),
        "installments": mp.get("installments"),
        "payer_id": ((mp.get("payer") or {}).get("id")) if isinstance(mp.get("payer"), dict) else None,
        "payer_email": ((mp.get("payer") or {}).get("email")) if isinstance(mp.get("payer"), dict) else None,
        "external_reference": mp.get("external_reference"),
        "order_id": ((mp.get("order") or {}).get("id")) if isinstance(mp.get("order"), dict) else None,
        "live_mode": mp.get("live_mode"),
        # created_at / updated_at son manejados por DB si las tienes con defaults/triggers; upsert las deja intactas
    }

def map_payment_payloads_row(payment_id: int, payload: dict) -> dict:
    """
    Tu tabla payment_payloads tiene 'payment_id' (PK/FK) y 'raw_payment' (jsonb NOT NULL).
    """
    return {
        "payment_id": payment_id,
        "raw_payment": json.dumps(payload),  # 👈 columna correcta, NOT NULL
        # 'fetched_at' tiene DEFAULT now() en DB (si existe)
    }

def map_antifraud_row(mp: dict) -> dict:
    card = mp.get("card") or {}
    cardholder = card.get("cardholder") or {}
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "card_first_six_digits": card.get("first_six_digits"),
        "card_last_four_digits": card.get("last_four_digits"),
        "cardholder_name": cardholder.get("name"),
        # Si capturas esto en tu checkout propio, podrás poblarlos también:
        # "payer_ip": ...,
        # "user_agent": ...,
        # "device_id": ...,
        # "ticket_number": ...,
    }

# ========= Webhook persistence =========
def save_webhook_event_first(payload: dict, event_id: str, topic: str):
    """
    Insert inicial SIEMPRE con payment_id=NULL para no romper FK.
    """
    row = {
        "event_id": event_id,
        "topic": topic or "unknown",
        "payment_id": None,                     # 👈 clave para no violar la FK
        "raw_payload": json.dumps(payload),     # tu tabla webhook_events sí tiene raw_payload jsonb
        "attempt": 1                            # si existe, lo usará; si no, lo ignora
    }
    upsert_row("webhook_events", ["event_id"], row)

def finalize_webhook_event(event_id: str, fields: dict):
    update_row_fields("webhook_events", "event_id", event_id, fields)

# ========= Flujo principal =========
def process_payment_flow(event_id: str, topic: str, webhook_payload: dict):
    # 1) Guardar SIEMPRE el evento con payment_id=NULL
    save_webhook_event_first(webhook_payload, event_id, topic)

    pid = (
        _to_bigint_or_none(webhook_payload.get("payment_id"))
        or _to_bigint_or_none(webhook_payload.get("id"))
        or _to_bigint_or_none((webhook_payload.get("data") or {}).get("id"))
    )

    if not pid:
        finalize_webhook_event(event_id, {"processed_at": psy_now()})
        print(f"ℹ️ Webhook {event_id} sin payment_id; fin.", flush=True)
        return

    # 2) Obtener el payment desde MP (si el id es real y tienes token)
    mp_payment = get_payment_from_mp(pid)

    # 3) Si obtuvimos payment → upsert en payments
    payments_ok = False
    if mp_payment:
        try:
            payments_ok = upsert_row("payments", ["payment_id"], map_payments_row(mp_payment))
            if payments_ok:
                print(f"💾 payments upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payments falló:", e, flush=True)

    # 4) SOLO si payments_ok → guardar payload de la API en payment_payloads.raw_payment
    if payments_ok and mp_payment:
        try:
            upsert_row("payment_payloads", ["payment_id"], map_payment_payloads_row(pid, mp_payment))
            print(f"💾 payment_payloads upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_payloads falló:", e, flush=True)

        # Antifraude (opcional, no sensible)
        try:
            upsert_row("payment_antifraud", ["payment_id"], map_antifraud_row(mp_payment))
            print(f"💾 payment_antifraud upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_antifraud falló:", e, flush=True)

        # ✅ Ya que existe payments, ahora sí actualizamos el evento con payment_id
        finalize_webhook_event(event_id, {"payment_id": pid, "processed_at": psy_now()})
    else:
        # No se pudo obtener/guardar payment → NO tocar payment_id del evento (evita violar la FK)
        status = "payment_not_found" if not mp_payment else "payments_upsert_failed"
        finalize_webhook_event(event_id, {"processed_at": psy_now()})
        print(f"ℹ️ {status} (event_id={event_id}, payment_id={pid})", flush=True)

# ========= Endpoints =========
@app.route("/webhook", methods=["POST"])
def webhook():
    payload = unify_payload(request)
    topic = (payload.get("topic") or "unknown")
    event_id = (
        payload.get("id")
        or payload.get("event_id")
        or (payload.get("data") or {}).get("id")
        or f"{topic}-{int(time.time()*1000)}"
    )
    print("📦 Webhook recibido:", {"event_id": event_id, "topic": topic}, flush=True)

    try:
        process_payment_flow(event_id, topic, payload)
    except Exception as e:
        # Estado final si algo revienta
        finalize_webhook_event(event_id, {"processed_at": psy_now()})
        print("❌ Error procesando webhook:", e, flush=True)

    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "OK — Webhook listo con payments/payloads/antifraud (robusto) 🚀", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
