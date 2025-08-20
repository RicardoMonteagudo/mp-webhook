from flask import Flask, request
import os, requests

app = Flask(__name__)

MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")  # <-- ponlo como variable de entorno en Cloud Run

@app.route("/webhook", methods=["POST"])
def webhook():
    # 1) Leer datos sin importar el content-type
    body = {}
    if request.is_json:
        body = request.get_json(silent=True) or {}
    else:
        # IPN suele venir como form-encoded o incluso solo en la query string
        body = request.form.to_dict() if request.form else {}

    # 2) También leer query string (IPN acostumbra enviar ?topic=...&id=...)
    topic = body.get("topic") or request.args.get("topic")
    resource_id = body.get("id") or request.args.get("id")

    print("📩 Notificación recibida | body:", body, "| args:", dict(request.args), flush=True)

    # 3) Si es un pago, traer los detalles (monto, estado, etc.)
    if topic == "payment" and resource_id and MP_ACCESS_TOKEN:
        try:
            url = f"https://api.mercadopago.com/v1/payments/{resource_id}"
            headers = {"Authorization": f"Bearer {MP_ACCESS_TOKEN}"}
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            payment = r.json()

            # Datos útiles
            status = payment.get("status")
            amount = payment.get("transaction_amount")
            payer_email = (payment.get("payer") or {}).get("email")
            description = payment.get("description")

            print(f"✅ Pago {resource_id} | status={status} | amount={amount} | email={payer_email} | desc={description}", flush=True)

            # TODO: aquí guarda en tu DB o dispara tu lógica
        except Exception as e:
            print("⚠️ Error consultando pago:", e, flush=True)

    # 4) Responder 200 para que MP no reintente
    return "ok", 200


@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200
