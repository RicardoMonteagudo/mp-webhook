from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("🔔 Notificación recibida:", data)
    # Aquí más adelante procesas la notificación de Mercado Pago
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
