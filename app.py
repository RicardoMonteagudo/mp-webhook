from flask import Flask, request
import os, json, time
import psycopg2
import psycopg2.extras
import requests
from decimal import Decimal

app = Flask(__name__)

# ====== ENV ======
INSTANCE   = os.environ["INSTANCE_CONNECTION_NAME"]
DB_NAME    = os.environ["DB_NAME"]
DB_USER    = os.environ["DB_USER"]
DB_PASS    = os.environ["DB_PASS"]
DB_SCHEMA  = os.getenv("DB_SCHEMA", "baikarool")
MP_TOKEN   = os.getenv("MP_ACCESS_TOKEN")  # opcional pero recomendado para /v1/payments/{id}

# ====== DB ======
def get_conn():
    """Conexión vía UNIX socket a Cloud SQL (Postgres)."""
    return psycopg2.connect(
        host=f"/cloudsql/{INSTANCE}",
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=5432,
        options=f"-c search_path={DB_SCHEMA},public"
    )

def get_table_columns(table: str) -> set:
    """Obtiene columnas reales de una tabla para hacer inserciones seguras."""
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
    UPSERT seguro: solo incluye columnas existentes y con valor no-None.
    Evita sobreescribir con NULL y no truena si faltan columnas.
    """
    cols_in_db = get_table_columns(table)
    data = {k: v for k, v in row.items() if v is not None and k in cols_in_db}
    if not data:
        print(f"⚠️ Nada que upsert en {table} (no hay columnas válidas con valor).", flush=True)
        return

    cols = list(data.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(cols)
    pk_list = ", ".join(pk_cols)

    # Para DO UPDATE, solo columnas no-PK
    update_cols = [c for c in cols if c not in pk_cols]
    if update_cols:
        setlist = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
        sql = f"""
        INSERT INTO {DB_SCHEMA}.{table} ({collist})
        VALUES ({placeholders})
        ON CONFLICT ({pk_list}) DO UPDATE
        SET {setlist}
        """
    else:
        # Si solo vienen PKs, no hay nada que actualizar
        sql = f"""
        INSERT INTO {DB_SCHEMA}.{table} ({collist})
        VALUES ({placeholders})
        ON CONFLICT ({pk_list}) DO NOTHING
        """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [data[c] for c in cols])

def update_row_fields(table: str, where_k: str, where_v, fields: dict):
    cols_in_db = get_table_columns(table)
    data = {k: v for k, v in fields.items() if k in cols_in_db}
    if not data:
        return
    setlist = ", ".join([f"{k}=%s" for k in data.keys()])
    sql = f"UPDATE {DB_SCHEMA}.{table} SET {setlist} WHERE {where_k}=%s"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, list(data.values()) + [where_v])

# ====== Helpers ======
def _to_bigint_or_none(val):
    if val is None: return None
    try:
        return int(str(val))
    except Exception:
        return None

def _to_decimal_or_none(val):
    if val is None: return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

def unify_payload(req) -> dict:
    """Combina JSON + form + query sin forzar Content-Type."""
    payload = {}
    if req.is_json:
        payload.update(req.get_json(silent=True) or {})
    payload.update(req.form.to_dict())
    payload.update(req.args.to_dict())
    return payload

# ====== Mercado Pago API ======
def get_payment_from_mp(payment_id: int) -> dict | None:
    if not (MP_TOKEN and payment_id):
        return None
    url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {MP_TOKEN}"}, timeout=10)
        if r.status_code == 200:
            return r.json()
        print(f"⚠️ MP /v1/payments/{payment_id} status={r.status_code} body={r.text[:500]}", flush=True)
    except Exception as e:
        print("⚠️ Error consultando MP:", e, flush=True)
    return None

# ====== Mapeos ======
def map_payments_row(mp: dict) -> dict:
    """
    Mapea el JSON de MP /v1/payments/{id} a tu tabla 'payments'.
    Solo campos canónicos; usa los que existan en tu DB.
    """
    # Campos raíz estándar
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "status": (mp.get("status") or "").lower() or None,
        "status_detail": mp.get("status_detail"),
        "amount": _to_decimal_or_none(mp.get("transaction_amount")),
        "currency": mp.get("currency_id"),
        # En tu schema actual tienes 'method' (texto). Usamos payment_method_id o payment_type_id.
        "method": mp.get("payment_method_id") or mp.get("payment_type_id"),
        "approved_at": mp.get("date_approved"),
        # 'created_at' ya tiene DEFAULT now(); si quisieras fidelidad: mp.get("date_created")
        "payer_email": ((mp.get("payer") or {}).get("email")) if isinstance(mp.get("payer"), dict) else None,
        # Campos “extra” solo si tu tabla ya los tiene (upsert los filtrará):
        "external_reference": mp.get("external_reference"),
        "order_id": ((mp.get("order") or {}).get("id")) if isinstance(mp.get("order"), dict) else None,
        "payment_method_id": mp.get("payment_method_id"),
        "payment_type_id": mp.get("payment_type_id"),
        "installments": mp.get("installments"),
        "issuer_id": ((mp.get("issuer_id")) or ((mp.get("issuer") or {}).get("id")) if isinstance(mp.get("issuer"), dict) else None),
        "captured": mp.get("captured"),
        # fee_details puede venir como lista; aquí solo ejemplo del total si existiera
        "fee_amount": _to_decimal_or_none((mp.get("fee_details") or [{}])[0].get("amount")) if isinstance(mp.get("fee_details"), list) and mp["fee_details"] else None,
        "net_received_amount": _to_decimal_or_none((mp.get("transaction_details") or {}).get("net_received_amount")),
        "refunded_amount": _to_decimal_or_none(mp.get("refund_amount")),
        "statement_descriptor": mp.get("statement_descriptor"),
    }

def map_payment_payloads_row(payment_id: int, payload: dict, source: str) -> dict:
    """
    Prepara fila para 'payment_payloads' con el JSON del payment (preferible) o del webhook.
    Nota: PK es payment_id; si ya existe, se reemplaza raw_payload.
    """
    return {
        "payment_id": payment_id,
        "topic": (payload.get("topic") or "payment") if isinstance(payload, dict) else "payment",
        "raw_payload": json.dumps(payload),
        # Campos adicionales si existen en tu tabla:
        "payload_type": "payment_api" if source == "api" else "webhook",
        "source": "mp_api" if source == "api" else "mp_webhook",
        # "headers": json.dumps(...),  # si después quieres guardar headers del webhook
    }

def map_antifraud_row(mp: dict) -> dict:
    """
    Señales no sensibles para 'payment_antifraud':
    - BIN (first6), last4, cardholder_name, issuer_id.
    No guardamos PAN completo/CVV por PCI.
    """
    card = mp.get("card") or {}
    cardholder = card.get("cardholder") or {}
    return {
        "payment_id": _to_bigint_or_none(mp.get("id")),
        "card_first_six_digits": card.get("first_six_digits"),
        "card_last_four_digits": card.get("last_four_digits"),
        "cardholder_name": cardholder.get("name"),
        "issuer_id": (mp.get("issuer_id") or (mp.get("issuer") or {}).get("id") if isinstance(mp.get("issuer"), dict) else None),
        # Si capturas IP/UA del CLIENTE en tu checkout, los puedes mapear aquí.
        # Desde el webhook de MP NO vienen IP/UA del cliente.
        # "ip": ...,
        # "user_agent": ...,
    }

# ====== Persistencia principal ======
def save_webhook_event_first(payload: dict, event_id: str, topic: str):
    """
    Inserta el webhook en webhook_events SIN payment_id para no romper la FK.
    Después, si logramos upsert de payments, actualizamos payment_id y process_status.
    """
    row = {
        "event_id": event_id,
        "topic": topic,
        "payment_id": None,                 # clave: evitar FK por ahora
        "raw_payload": json.dumps(payload),
        # received_at tiene DEFAULT now(), attempt DEFAULT 1
        "process_status": "received",
    }
    # Si tu tabla aún no tiene process_status, el upsert filtrará y no fallará.
    upsert_row("webhook_events", ["event_id"], row)

def finalize_webhook_event(event_id: str, fields: dict):
    update_row_fields("webhook_events", "event_id", event_id, fields)

# ====== Handler principal ======
def process_payment_flow(event_id: str, topic: str, webhook_payload: dict):
    """
    Flujo: 1) Guarda webhook, 2) si hay payment_id, consulta API y upsert payments,
    3) guarda payment_payloads (API), 4) guarda antifraud, 5) actualiza webhook_events.
    """
    save_webhook_event_first(webhook_payload, event_id, topic)

    pid = (
        _to_bigint_or_none(webhook_payload.get("payment_id"))
        or _to_bigint_or_none(webhook_payload.get("id"))
        or _to_bigint_or_none((webhook_payload.get("data") or {}).get("id"))
    )

    if not pid:
        finalize_webhook_event(event_id, {"processed_at": psy_now(), "process_status": "processed_no_pid"})
        print(f"ℹ️ Webhook {event_id} sin payment_id; flujo terminado.", flush=True)
        return

    # 1) Intentar traer payment de la API
    mp_payment = get_payment_from_mp(pid)

    if mp_payment:
        # 2) UPSERT en payments
        try:
            upsert_row("payments", ["payment_id"], map_payments_row(mp_payment))
            print(f"💾 payments upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payments falló:", e, flush=True)

        # 3) payment_payloads con payload de la API (más completo)
        try:
            upsert_row("payment_payloads", ["payment_id"], map_payment_payloads_row(pid, mp_payment, source="api"))
            print(f"💾 payment_payloads upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_payloads falló:", e, flush=True)

        # 4) antifraud señales no sensibles
        try:
            upsert_row("payment_antifraud", ["payment_id"], map_antifraud_row(mp_payment))
            print(f"💾 payment_antifraud upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_antifraud falló:", e, flush=True)

        # 5) Actualizar webhook_events con payment_id y status procesado
        finalize_webhook_event(event_id, {
            "payment_id": pid,
            "processed_at": psy_now(),
            "process_status": "processed"
        })
    else:
        # Si no hubo API, intenta al menos guardar el payload del webhook en payment_payloads (si trae pid)
        try:
            upsert_row("payment_payloads", ["payment_id"], map_payment_payloads_row(pid, webhook_payload, source="webhook"))
            print(f"💾 payment_payloads (webhook) upsert ok (payment_id={pid})", flush=True)
        except Exception as e:
            print("⚠️ upsert payment_payloads (webhook) falló:", e, flush=True)

        finalize_webhook_event(event_id, {
            "payment_id": pid,
            "processed_at": psy_now(),
            "process_status": "received_api_unavailable"
        })
        print(f"ℹ️ No se obtuvo payment de la API; webhook registrado (payment_id={pid}).", flush=True)

def psy_now():
    """Devuelve 'now()' evaluado en Postgres (útil si quieres forzar timestamps del servidor DB)."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT now()")
        return cur.fetchone()[0]

# ====== Flask endpoints ======
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
        # Si algo truena, deja el evento como failed
        finalize_webhook_event(event_id, {
            "processed_at": psy_now(),
            "process_status": "failed",
            "error_message": str(e)[:500]
        })
        print("❌ Error procesando webhook:", e, flush=True)

    # Responder rápido a MP
    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "OK — Webhook + Payments + Antifraud listo 🚀", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
