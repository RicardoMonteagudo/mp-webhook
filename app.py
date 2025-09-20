from flask import Flask, request
import os, json, time
import psycopg2, requests
from decimal import Decimal
import paho.mqtt.client as mqtt

app = Flask(__name__)

# ===== ENV =====
INSTANCE   = os.environ["INSTANCE_CONNECTION_NAME"]
DB_NAME    = os.environ["DB_NAME"]
DB_USER    = os.environ["DB_USER"]
DB_PASS    = os.environ["DB_PASS"]
DB_SCHEMA  = os.getenv("DB_SCHEMA", "baikarool")
MP_TOKEN   = os.getenv("MP_ACCESS_TOKEN")

# ===== MOSQUITTO =====

MQTT_HOST  = os.environ["baikarool_MQTT_HOST"]
MQTT_PORT  = int(os.environ["baikarool_MQTT_PORT"])
MQTT_USER  = os.environ["baikarool_MQTT_USER"]
MQTT_PASS  = os.environ["baikarool_MQTT_PASS"]
MQTT_TOPIC = os.environ["baikarool_MQTT_TOPIC"]

#def mqtt_publish(msg: dict):
 #   try:
  #      c = mqtt.Client(client_id="cloudrun-pub", protocol=mqtt.MQTTv5)
   #     c.username_pw_set(MQTT_USER, MQTT_PASS)   # 🔑 user/pass
    #    c.tls_set()                               # 🔒 TLS en 8883
     #   c.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
      #  c.publish(MQTT_TOPIC, json.dumps(msg), qos=1)
       # c.disconnect()
    #except Exception as e:
     #   print("MQTT error:", e, flush=True)
def mqtt_publish(msg: dict):
    try:
        print(f"MQTT → {MQTT_HOST}:{MQTT_PORT} topic={MQTT_TOPIC}", flush=True)
        c = mqtt.Client(client_id="cloudrun-pub", protocol=mqtt.MQTTv311)  # 👈 v3.1.1
        c.username_pw_set(MQTT_USER, MQTT_PASS)
        c.tls_set()                 # usa CA del sistema
        c.tls_insecure_set(True)    # 👈 TEMP: evita fallo de cert en Cloud Run
        c.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
        r = c.publish(MQTT_TOPIC, json.dumps(msg), qos=1)
        r.wait_for_publish()
        print("MQTT published:", r.is_published(), flush=True)
        c.disconnect()
    except Exception as e:
        print("MQTT error:", repr(e), flush=True)

def is_accredited_in_sql(pid: int) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT status, status_detail FROM {DB_SCHEMA}.payments WHERE payment_id=%s", (pid,))
        r = cur.fetchone()
        return r and (r[0] or "").lower()=="approved" and (r[1] or "").lower()=="accredited"

# ===== DB =====
def get_conn():
    return psycopg2.connect(
        host=f"/cloudsql/{INSTANCE}",
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=5432,
        options=f"-c search_path={DB_SCHEMA},public"
    )

def psy_now():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT now()"); return cur.fetchone()[0]

def get_table_columns(table: str) -> set:
    sql = """SELECT column_name FROM information_schema.columns
             WHERE table_schema=%s AND table_name=%s"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (DB_SCHEMA, table))
        return {r[0] for r in cur.fetchall()}

def upsert_row(table: str, pk_cols, row: dict) -> bool:
    cols_in_db = get_table_columns(table)
    data = {k: v for k, v in row.items() if v is not None and k in cols_in_db}
    if not data: return False
    cols = list(data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(cols)
    pk_list = ", ".join(pk_cols)
    update_cols = [c for c in cols if c not in pk_cols]
    if update_cols:
        setlist = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
        sql = f"INSERT INTO {DB_SCHEMA}.{table} ({collist}) VALUES ({placeholders}) " \
              f"ON CONFLICT ({pk_list}) DO UPDATE SET {setlist}"
    else:
        sql = f"INSERT INTO {DB_SCHEMA}.{table} ({collist}) VALUES ({placeholders}) " \
              f"ON CONFLICT ({pk_list}) DO NOTHING"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [data[c] for c in cols])
    return True

def update_row_fields(table: str, where_k: str, where_v, fields: dict):
    cols_in_db = get_table_columns(table)
    data = {k: v for k, v in fields.items() if k in cols_in_db}
    if not data: return
    setlist = ", ".join([f"{k}=%s" for k in data.keys()])
    sql = f"UPDATE {DB_SCHEMA}.{table} SET {setlist} WHERE {where_k}=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, list(data.values()) + [where_v])

# ===== Helpers =====
def _to_bigint_or_none(v):
    try: return int(str(v)) if v is not None else None
    except Exception: return None

def _to_dec_or_none(v):
    try: return Decimal(str(v)) if v is not None else None
    except Exception: return None

def _json(d, *path):
    cur = d
    for k in path:
        if not isinstance(cur, dict): return None
        cur = cur.get(k)
    return cur

def unify_payload(req) -> dict:
    p = {}
    if req.is_json: p.update(req.get_json(silent=True) or {})
    p.update(req.form.to_dict()); p.update(req.args.to_dict()); return p

# ===== MP API =====
def get_payment_from_mp(payment_id: int) -> dict | None:
    if not (MP_TOKEN and payment_id): return None
    url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {MP_TOKEN}"}, timeout=10)
        if r.status_code == 200: return r.json()
        print(f"⚠️ GET {url} -> {r.status_code} {r.text[:300]}", flush=True)
    except Exception as e:
        print("⚠️ MP API error:", e, flush=True)
    return None

# ===== Mapeos =====
def map_payments_row(mp: dict) -> dict:
    fees = mp.get("fee_details")
    fee_amount = None
    if isinstance(fees, list) and fees:
        try:
            fee_amount = sum(Decimal(str(i.get("amount"))) for i in fees if i.get("amount") is not None)
        except Exception:
            fee_amount = None
    pos_id   = mp.get("pos_id")   or _json(mp, "point_of_interaction", "business_info", "pos_id")
    store_id = mp.get("store_id") or _json(mp, "point_of_interaction", "business_info", "store_id")
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "status": (mp.get("status") or "").lower() or None,
        "status_detail": mp.get("status_detail"),
        "amount": _to_dec_or_none(mp.get("transaction_amount")),
        "currency": mp.get("currency_id"),
        "date_created": mp.get("date_created"),
        "date_approved": mp.get("date_approved"),
        "payment_method_id": mp.get("payment_method_id"),
        "installments": mp.get("installments"),
        "payer_id": _to_bigint_or_none(_json(mp, "payer", "id")),
        "payer_email": _json(mp, "payer", "email"),
        "external_reference": mp.get("external_reference"),
        "order_id": _json(mp, "order", "id"),
        "live_mode": mp.get("live_mode"),
        "payment_type_id": mp.get("payment_type_id"),
        "issuer_id": mp.get("issuer_id") or _json(mp, "issuer", "id"),
        "pos_id": pos_id,
        "store_id": store_id,
        "installment_amount": _to_dec_or_none(_json(mp, "transaction_details", "installment_amount")),
        "operation_type": mp.get("operation_type"),
        "date_accredited": mp.get("money_release_date") or mp.get("date_accredited"),
        "net_received_amount": _to_dec_or_none(_json(mp, "transaction_details", "net_received_amount")),
        "fee_amount": fee_amount,
        "settlement_currency": _json(mp, "transaction_details", "settlement_currency") or mp.get("settlement_currency"),
    }

def map_payment_payloads_row(payment_id: int, payload: dict) -> dict:
    return {"payment_id": payment_id, "raw_payment": json.dumps(payload)}

def map_antifraud_row(mp: dict) -> dict:
    card = mp.get("card") or {}; cardholder = card.get("cardholder") or {}
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "card_first_six_digits": card.get("first_six_digits"),
        "card_last_four_digits": card.get("last_four_digits"),
        "cardholder_name": cardholder.get("name"),
        "risk_level": _json(mp, "risk_execution_result", "level") or mp.get("risk_level"),
        "risk_reason": _json(mp, "risk_execution_result", "reason") or mp.get("risk_reason"),
    }

# ===== Webhook helpers =====
def save_webhook_event_first(payload: dict, event_id: str, topic: str):
    upsert_row("webhook_events", ["event_id"], {
        "event_id": event_id, "topic": topic or "unknown",
        "payment_id": None, "raw_payload": json.dumps(payload), "attempt": 1
    })

def finalize_webhook_event(event_id: str, fields: dict):
    update_row_fields("webhook_events", "event_id", event_id, fields)

# ===== Flujos =====
def process_payment_event(event_id: str, payload: dict):
    save_webhook_event_first(payload, event_id, "payment")
    pid = _to_bigint_or_none(payload.get("payment_id")) \
          or _to_bigint_or_none(_json(payload, "data", "id")) \
          or _to_bigint_or_none(payload.get("id"))
    if not pid:
        finalize_webhook_event(event_id, {"processed_at": psy_now()}); return

    mp_payment = get_payment_from_mp(pid)
    if not mp_payment:
        finalize_webhook_event(event_id, {"processed_at": psy_now()}); return

    if upsert_row("payments", ["payment_id"], map_payments_row(mp_payment)):
        upsert_row("payment_payloads", ["payment_id"], map_payment_payloads_row(pid, mp_payment))
        upsert_row("payment_antifraud", ["payment_id"], map_antifraud_row(mp_payment))
        finalize_webhook_event(event_id, {"payment_id": pid, "processed_at": psy_now()})

        if is_accredited_in_sql(pid):
            mqtt_publish({"type":"blink3","payment_id": pid})
        
    else:
        finalize_webhook_event(event_id, {"processed_at": psy_now()})

def process_chargeback_event(event_id: str, payload: dict):
    save_webhook_event_first(payload, event_id, "chargebacks")
    cb_id = str(payload.get("id") or payload.get("chargeback_id") or "")
    pid   = _to_bigint_or_none(payload.get("payment_id")) \
            or _to_bigint_or_none(_json(payload, "data", "payment_id")) \
            or _to_bigint_or_none(_json(payload, "chargeback", "payment_id"))
    status = payload.get("status") or _json(payload, "data", "status") or _json(payload, "chargeback", "status")
    reason = payload.get("reason") or payload.get("reason_code") or _json(payload, "data", "reason_code")
    if cb_id and pid:
        upsert_row("payment_chargebacks", ["chargeback_id"], {
            "chargeback_id": cb_id, "payment_id": pid,
            "status": status, "reason_code": reason
        })
        finalize_webhook_event(event_id, {"payment_id": pid, "processed_at": psy_now()})
    else:
        finalize_webhook_event(event_id, {"processed_at": psy_now()})

# ===== Endpoints =====
@app.route("/webhook", methods=["POST"])
def webhook():
    p = unify_payload(request)
    topic = (p.get("topic") or "").lower()
    event_id = p.get("id") or p.get("event_id") or _json(p, "data", "id") or f"{topic}-{int(time.time()*1000)}"
    try:
        if topic == "payment":
            process_payment_event(event_id, p)
        elif topic == "chargebacks":
            process_chargeback_event(event_id, p)
        else:
            save_webhook_event_first(p, event_id, topic or "unknown")
            finalize_webhook_event(event_id, {"processed_at": psy_now()})
    except Exception as e:
        finalize_webhook_event(event_id, {"processed_at": psy_now()})
        print("❌ Error procesando webhook:", e, flush=True)
    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "OK — payments/payloads/antifraud + chargebacks (sin refunds) 🚀", 200

@app.route("/test-blink", methods=["GET"])
def test_blink():
    print(f"MQTT test → {MQTT_TOPIC}", flush=True)
    mqtt_publish({"type":"blink3","from":"test"})
    return ("ok", 200)
