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
MP_TOKEN   = os.getenv("MP_ACCESS_TOKEN")  # necesario para /v1/payments/{id}

# ========= DB =========
def get_conn():
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
def _json(mp, *path):
    cur = mp
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def map_payments_row(mp: dict) -> dict:
    """
    Mapea los campos que acordamos (si existen en la respuesta de MP):
    - neto, comisiones, granularidad, POS/terminal, cuotas detalladas, tipo de operación, tiempos, moneda de liquidación
    """
    # Finanzas
    neto = _to_decimal_or_none(_json(mp, "transaction_details", "net_received_amount"))
    # fee_details es una lista; sumamos amounts si vienen
    fees = _json(mp, "fee_details")
    fee_amount = None
    if isinstance(fees, list) and fees:
        try:
            fee_amount = sum(Decimal(str(i.get("amount"))) for i in fees if i.get("amount") is not None)
        except Exception:
            fee_amount = None

    # POS / Terminal: MP puede exponer pos_id/store_id arriba o dentro de point_of_interaction
    pos_id = mp.get("pos_id") or _json(mp, "point_of_interaction", "business_info", "pos_id")
    store_id = mp.get("store_id") or _json(mp, "point_of_interaction", "business_info", "store_id")

    # Cuotas detalladas (si existe un monto por cuota en transaction_details)
    installment_amount = _to_decimal_or_none(_json(mp, "transaction_details", "installment_amount"))

    # Fechas de proceso
    date_accredited = mp.get("money_release_date") or mp.get("date_accredited")

    # Moneda de liquidación (si MP la expone en algún subobjeto; si no, queda NULL)
    settlement_currency = _json(mp, "transaction_details", "settlement_currency") or mp.get("settlement_currency")

    return {
        # ya existentes en tu tabla
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

        # nuevos campos
        "payment_type_id": mp.get("payment_type_id"),
        "issuer_id": (mp.get("issuer_id") or _json(mp, "issuer", "id")),
        "pos_id": pos_id,
        "store_id": store_id,
        "installment_amount": installment_amount,
        "operation_type": mp.get("operation_type"),
        "date_accredited": date_accredited,
        "net_received_amount": neto,
        "fee_amount": fee_amount,
        "settlement_currency": settlement_currency,
    }

def map_payment_payloads_row(payment_id: int, payload: dict) -> dict:
    # Tu tabla exige 'raw_payment' NOT NULL
    return {
        "payment_id": payment_id,
        "raw_payment": json.dumps(payload),
    }

def map_antifraud_row(mp: dict) -> dict:
    card = mp.get("card") or {}
    cardholder = card.get("cardholder") or {}
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "card_first_six_digits": card.get("first_six_digits"),
        "card_last_four_digits": card.get("last_four_digits"),
        "cardholder_name": cardholder.get("name"),
        # Riesgo/flags si MP los expone
        "risk_level": _json(mp, "risk_execution_result", "level") or mp.get("risk_level"),
        "risk_reason": _json(mp, "risk_execution_result", "reason") or mp.get("risk_reason"),
    }

# ========= Webhook =========
def save_webhook_event_first(payload: dict, event_id: str, topic: str):
    row = {
        "event_id": event_id,
        "topic": topic or "unknown",
        "payment_id": None,
        "raw_payload": json.dumps(payload),
        "attempt": 1
    }
    upsert_row("webhook_events", ["event_id"], row)

def finalize_webhook_event(event_id: str, fields: dict):
    update_row_fields("webhook_events", "event_id", event_id, fields)

def process_payment_flow(event_id: str, topic: str, webhook_payload: dict):
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

    mp_payment = get_payment_from_mp(pid)

    payments_ok = False
    if mp_payment:
        try:
            payments_ok = upsert_row("payments", ["payment_id"], map_payments_row(mp_payment))
            if payments_ok:
                print(f"💾 payments upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payments falló:", e, flush=True)

    if payments_ok and mp_payment:
        try:
            upsert_row("payment_payloads", ["payment_id"], map_payment_payloads_row(pid, mp_payment))
            print(f"💾 payment_payloads upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_payloads falló:", e, flush=True)

        try:
            upsert_row("payment_antifraud", ["payment_id"], map_antifraud_row(mp_payment))
            print(f"💾 payment_antifraud upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_antifraud falló:", e, flush=True)

        finalize_webhook_event(event_id, {"payment_id": pid, "processed_at": psy_now()})
    else:
        finalize_webhook_event(event_id, {"processed_at": psy_now()})
        print(f"ℹ️ payment no disponible/guardado (event_id={event_id}, payment_id={pid})", flush=True)

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
        finalize_webhook_event(event_id, {"processed_at": psy_now()})
        print("❌ Error procesando webhook:", e, flush=True)

    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "OK — Webhook + payments + payloads + antifraud (ampliado) 🚀", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
