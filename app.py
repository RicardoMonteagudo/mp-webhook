from flask import Flask, request

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("📥 Notificación recibida:", data, flush=True)  # Esto aparece en Cloud Run logs
    return "ok", 200

@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200
