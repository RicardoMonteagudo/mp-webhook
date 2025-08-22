from flask import Flask

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Hola Ricardo 🚀 Tu webhook está vivo!", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    return "Recibido ✅", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
