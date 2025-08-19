from flask import Flask, request, jsonify
import os

app = Flask(__name__)

# Obtén la clave secreta desde variables de entorno
SECRET_KEY = os.getenv("MP_SECRET_KEY", "tu_clave_secreta")

@app.route("/")
def home():
    return "Webhook funcionando 🚀"

@app.route("/webhook", methods=["POST"])
def webhook():
    # Validar la clave secreta
    signature = request.headers.get("x-signature", "")
    if signature != SECRET_KEY:
        return jsonify({"error": "No autorizado"}), 401

    # Captura el JSON que manda Mercado Pago
    data = request.json
    print("📩 Notificación recibida:", data)

    # Aquí puedes guardar en tu base de datos o procesar el evento
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
