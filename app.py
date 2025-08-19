from flask import Flask, request

app = Flask(__name__)

# Acepta GET y POST tanto en "/" como en "/webhook"
@app.route("/", methods=["GET", "POST"])
@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    # Log muy verboso para ver qué llega
    body = request.get_data(as_text=True)  # crudo
    print("📥 METHOD:", request.method)
    print("📥 HEADERS:", dict(request.headers))
    print("📥 BODY:", body)
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
