from flask import Flask, request
import os, requests

app = Flask(__name__)

MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN")  # opcional

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
