from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200

# Acepta POST tanto en / como en /webhook (por si MP pega a la raíz)
@app.route("/", methods=["POST"])
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("📩 Notificación recibida:", data)
    return jsonify({"status": "ok"}), 200
