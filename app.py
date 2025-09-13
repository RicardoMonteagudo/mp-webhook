from flask import Flask, request
import os, json, time
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# ===== DB envs =====
INSTANCE = os.environ["INSTANCE_CONNECTION_NAME"]
DB_NAME  = os.environ["DB_NAME"]
DB_USER  = os.environ["DB_USER"]
DB_PASS  = os.environ["DB_PASS"]
DB_SCHEMA = os.getenv("DB_SCHEMA", "baikarool")

def get_conn():
    """Conexión vía socket a Cloud SQL (Postgres)."""
    return psycopg2.connect(
        host=f"/cloudsql/{INSTANCE}",
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=5432,
        options=f"-c search_path={DB_SCHEMA},public"
    )

def save_webhook_to_events(payload: dict):
    """Inserta el webhook en la tabla webhook_events (prueba: ignoramos payment_id)."""
    topic = payload.get("topic") or "unknown"

    # event_id: si no viene, generamos uno
    event_id = (
        payload.get("id")
        or payload.get("event_id")
        or (payload.get("data") or {}).get("id")
        or f"{topic}-{int(time.time()*1000)}"
    )

    # 👇 En pruebas ignoramos payment_id para no romper la FK
    payment_id = None

    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"""
                    INSERT INTO {DB_SCHEMA}.webhook_events
                       (event_id, topic, payment_id, received_at, attempt, raw_payload)
                    VALUES
                       (%s, %s, %s, now(), %s, %s::jsonb)
                    """,
                    (str(event_id), topic, payment_id, 1, json.dumps(payload))
                )
        print(f"💾 Guardado en webhook_events → event_id={event_id}", flush=True)
    except Exception as e:
        print("❌ Error guardando webhook:", e, flush=True)

@app.route("/webhook", methods=["POST"])
def webhook():
    payload = {}
    if request.is_json:
        payload.update(request.get_json(silent=True) or {})
    payload.update(request.form.to_dict())
    payload.update(request.args.to_dict())

    print("📦 Payload recibido:", payload, flush=True)

    save_webhook_to_events(payload)

    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
