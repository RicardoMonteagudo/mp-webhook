from flask import Flask, request, jsonify
import os, json, requests, traceback
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import Json

app = Flask(__name__)

# ====== Config ======
DB_HOST   = os.getenv("DB_HOST")               # IP pública de Cloud SQL (o el hostname), ej: 34.xx.xx.xx
DB_NAME   = os.getenv("DB_NAME", "core_prod")
DB_USER   = os.getenv("DB_USER", "baikarool")
DB_PASS   = os.getenv("DB_PASS")
DB_SCHEMA = os.getenv("DB_SCHEMA", "baikarool")

MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")  # para consultar detalles del pago

POOL = SimpleConnectionPool(
    minconn=1,
    maxconn=int(os.getenv("DB_POOL_MAX", "5")),
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    connect_timeout=10,
    options=f"-c search_path={DB_SCHEMA},public"
)

def get_conn():
    return POOL.getconn()

def put_conn(conn):
    if conn:
        POOL.putconn(conn)

# ====== Utilidades ======
def pick_event_id(headers, payload):
    """
    Escoge un ID de evento estable para idempotencia.
    Preferimos header de request; si no, de payload.
    """
    # Headers útiles que a veces envían CDNs / proveedores
    for h in ("X-Request-Id", "X-Idempotency-Key", "X-Event-Id"):
        v = headers.get(h)
        if v:
            return f"h:{v}"
    # Payload (Mercado Pago notifica id/notification_id o data.id según flujo)
    for k in ("notification_id", "event_id", "id"):
        v = payload.get(k)
        if v:
            return f"p:{v}"
    # Recurso y tipo como fallback
    topic = payload.get("topic") or payload.get("type") or "unknown"
    rid = (
        payload.get("resource_id")
        or payload.get("data_id")
        or (payload.get("data", {}) or {}).get("id")
        or payload.get("id")
        or "noid"
    )
    return f"f:{topic}:{rid}"

def extract_topic_and_payment_id(payload):
    topic = payload.get("topic") or payload.get("type")
    # payment_id puede venir en varios lugares
    pid = (
        payload.get("id")
        or payload.get("resource_id")
        or payload.get("data_id")
        or (payload.get("data", {}) or {}).get("id")
    )
    return topic, pid

def fetch_payment_from_mp(payment_id):
    if not MP_ACCESS_TOKEN or not payment_id:
        return None, None
    url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"}, timeout=10)
    r.raise_for_status()
    return r.json(), r.status_code

def upsert_webhook_event(conn, event_id, topic, payment_id, raw_payload):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {DB_SCHEMA}.webhook_events (event_id, topic, payment_id, raw_payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, topic or "unknown", int(payment_id) if payment_id and str(payment_id).isdigit() else None, Json(raw_payload))
        )

def upsert_payment_core(conn, payment, rental_id_hint=None):
    """
    Inserta/actualiza payments con los campos clave.
    Si encontramos metadata.external_reference o metadata.rental_id, lo usamos para ligar la renta.
    """
    pid = payment.get("id")
    status = payment.get("status")
    status_detail = payment.get("status_detail")
    amount = payment.get("transaction_amount")
    currency = payment.get("currency_id")
    date_created = payment.get("date_created")
    date_approved = payment.get("date_approved")
    method_id = (payment.get("payment_method", {}) or {}).get("id") or payment.get("payment_method_id")
    installments = payment.get("installments")
    payer = payment.get("payer") or {}
    payer_id = payer.get("id")
    payer_email = payer.get("email")
    external_ref = payment.get("external_reference")
    order_id = (payment.get("order", {}) or {}).get("id")
    live_mode = payment.get("live_mode", False)

    # Rental id: prioridad a metadata.rental_id luego external_reference y por último hint
    metadata = payment.get("metadata") or {}
    rental_id = metadata.get("rental_id") or external_ref or rental_id_hint

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {DB_SCHEMA}.payments
            (payment_id, rental_id, status, status_detail, amount, currency,
             date_created, date_approved, payment_method_id, installments,
             payer_id, payer_email, external_reference, order_id, live_mode)
            VALUES
            (%(payment_id)s, %(rental_id)s, %(status)s, %(status_detail)s, %(amount)s, %(currency)s,
             %(date_created)s, %(date_approved)s, %(payment_method_id)s, %(installments)s,
             %(payer_id)s, %(payer_email)s, %(external_reference)s, %(order_id)s, %(live_mode)s)
            ON CONFLICT (payment_id) DO UPDATE SET
              rental_id = EXCLUDED.rental_id,
              status = EXCLUDED.status,
              status_detail = EXCLUDED.status_detail,
              amount = EXCLUDED.amount,
              currency = EXCLUDED.currency,
              date_created = EXCLUDED.date_created,
              date_approved = EXCLUDED.date_approved,
              payment_method_id = EXCLUDED.payment_method_id,
              installments = EXCLUDED.installments,
              payer_id = EXCLUDED.payer_id,
              payer_email = EXCLUDED.payer_email,
              external_reference = EXCLUDED.external_reference,
              order_id = EXCLUDED.order_id,
              live_mode = EXCLUDED.live_mode,
              updated_at = now()
            """,
            {
                "payment_id": pid,
                "rental_id": rental_id,
                "status": status,
                "status_detail": status_detail,
                "amount": amount,
                "currency": currency,
                "date_created": date_created,
                "date_approved": date_approved,
                "payment_method_id": method_id,
                "installments": installments,
                "payer_id": payer_id,
                "payer_email": payer_email,
                "external_reference": external_ref,
                "order_id": order_id,
                "live_mode": live_mode,
            }
        )

def upsert_payment_antifraud(conn, payment, user_agent_hdr, request_ip=None):
    pid = payment.get("id")
    card = payment.get("card") or {}
    first6 = card.get("first_six_digits")
    last4  = card.get("last_four_digits")
    holder = (card.get("cardholder") or {}).get("name")
    # IP puede venir de additional_info.ip_address o de headers
    ip = (payment.get("additional_info") or {}).get("ip_address") or request_ip

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {DB_SCHEMA}.payment_antifraud
            (payment_id, card_first_six_digits, card_last_four_digits, cardholder_name, payer_ip, user_agent, device_id, ticket_number)
            VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL)
            ON CONFLICT (payment_id) DO UPDATE SET
                card_first_six_digits = EXCLUDED.card_first_six_digits,
                card_last_four_digits = EXCLUDED.card_last_four_digits,
                cardholder_name = EXCLUDED.cardholder_name,
                payer_ip = EXCLUDED.payer_ip,
                user_agent = EXCLUDED.user_agent
            """,
            (pid, first6, last4, holder, ip, user_agent_hdr)
        )

def upsert_payment_payload(conn, payment):
    pid = payment.get("id")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {DB_SCHEMA}.payment_payloads (payment_id, raw_payment)
            VALUES (%s, %s)
            ON CONFLICT (payment_id) DO UPDATE SET
              raw_payment = EXCLUDED.raw_payment,
              fetched_at = now()
            """,
            (pid, Json(payment))
        )

def maybe_activate_rental(conn, payment):
    """
    Si el pago está 'approved' y sabemos a qué rental corresponde,
    marcamos la renta active y seteamos start_time (si estaba pending).
    (No enviamos comandos a hardware aquí, sólo el estado).
    """
    status = payment.get("status")
    if status != "approved":
        return
    metadata = payment.get("metadata") or {}
    rental_id = metadata.get("rental_id") or payment.get("external_reference")
    if not rental_id:
        return
    with conn.cursor() as cur:
        # Activa la renta si está pending
        cur.execute(
            f"""
            UPDATE {DB_SCHEMA}.rentals
               SET state = CASE WHEN state='pending' THEN 'active' ELSE state END,
                   start_time = CASE WHEN start_time IS NULL THEN now() ELSE start_time END,
                   updated_at = now()
             WHERE rental_id = %s
            """,
            (rental_id,)
        )

# ====== Rutas ======
@app.route("/webhook", methods=["POST"])
def webhook():
    # 1) Unificar payload
    payload = {}
    if request.is_json:
        payload.update(request.get_json(silent=True) or {})
    payload.update(request.form.to_dict())
    payload.update(request.args.to_dict())

    headers = {k: v for k, v in request.headers.items()}
    print("📩 Headers:", headers, flush=True)
    print("📦 Payload:", payload, flush=True)

    topic, resource_id = extract_topic_and_payment_id(payload)
    event_id = pick_event_id(headers, payload)

    # 2) Guardar webhook (idempotencia)
    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        upsert_webhook_event(conn, event_id, topic, resource_id, payload)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print("⚠️ Error guardando webhook_events:", e, traceback.format_exc(), flush=True)
        # Importante: MP reintenta si respondemos 500; aquí preferimos 200 y deduplicar luego
    finally:
        put_conn(conn)

    # 3) Si es pago, consultar detalle en MP y upsert en tablas
    if (topic == "payment" or topic == "payments") and resource_id:
        try:
            payment_json, _ = fetch_payment_from_mp(resource_id)
        except Exception as e:
            print("⚠️ Error consultando MP:", e, traceback.format_exc(), flush=True)
            payment_json = None

        if payment_json:
            # Ligamos por metadata.rental_id o external_reference si existen
            rental_hint = None
            meta = payment_json.get("metadata") or {}
            rental_hint = meta.get("rental_id") or payment_json.get("external_reference")

            try:
                conn = get_conn()
                conn.autocommit = False

                upsert_payment_core(conn, payment_json, rental_id_hint=rental_hint)
                ua = headers.get("User-Agent")
                req_ip = request.headers.get("X-Forwarded-For", request.remote_addr)
                upsert_payment_antifraud(conn, payment_json, ua, request_ip=req_ip)
                upsert_payment_payload(conn, payment_json)
                maybe_activate_rental(conn, payment_json)

                conn.commit()
            except Exception as e:
                if conn:
                    conn.rollback()
                print("⚠️ Error upsert pagos:", e, traceback.format_exc(), flush=True)
            finally:
                put_conn(conn)

    # 4) Responder rápido (MP reintenta solo si no recibe 200)
    return ("", 200)

@app.route("/", methods=["GET"])
def health():
    return jsonify(ok=True, service="mp-webhook", schema=DB_SCHEMA), 200
