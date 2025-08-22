from flask import Flask, request, abort
import os, requests, json, hmac, hashlib, time, threading

app = Flask(__name__)

# === ENV ===
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN", "")   # opcional: para GET /v1/payments/{id}
WEBHOOK_SECRET  = os.getenv("MP_SECRET_KEY", "")     # secreto del webhook (OBLIGATORIO)
# Políticas (puedes cambiarlas por envs si quieres)
REQUIRE_TOKEN_QS = False         # True si quieres exigir ?token=<secreto> en la URL
ALLOW_LEGACY_NO_HMAC = False     # True sólo para pruebas sin firma HMAC

# === Idempotencia simple en memoria ===
_processed = set()
_lock = threading.Lock()

def already_processed(req_id: str) -> bool:
    if not req_id:
        return False
    with _lock:
        if req_id in _processed:
            return True
        _processed.add(req_id)
        # Purga simple para no crecer sin límite
        if len(_processed) > 10000:
            _processed.clear()
        return False

def hmac_sha256(secret: str, data: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()

def parse_signature_header(sig_hdr: str):
    """
    Acepta formatos comunes:
      - 'sha256=<hex>'
      - 't=<unix_ts>, v1=<hex>'
      - 'ts=<unix_ts>,sha256=<hex>'
    Regresa (ts:int|None, hex:str|None)
    """
    if not sig_hdr:
        return None, None
    s = sig_hdr.strip()
    if s.startswith("sha256="):
        return None, s.split("=", 1)[1]
    parts = {}
    for chunk in s.replace(" ", "").split(","):
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            parts[k.lower()] = v
    ts = None
    if "t" in parts:
        try:
            ts = int(parts["t"])
        except:
            ts = None
    elif "ts" in parts:
        try:
            ts = int(parts["ts"])
        except:
            ts = None
    hexv = parts.get("v1") or parts.get("sha256")
    return ts, hexv

def verify_request(raw_body: bytes, headers, query_args):
    # 0) Checar que exista secreto
    if not WEBHOOK_SECRET:
        abort(500, "server missing secret")

    # 1) Token en query (si lo exiges)
    if REQUIRE_TOKEN_QS:
        token = query_args.get("token")
        if not token or token != WEBHOOK_SECRET:
            abort(401, "bad token")

    # 2) Firma HMAC (recomendada)
    sig_hdr = headers.get("X-Signature") or headers.get("x-signature")
    ts, sig_hex = parse_signature_header(sig_hdr)

    if sig_hex:
        # Si trae timestamp, verificar ventana de 5 minutos y firmar "ts.body"
        if ts:
            if abs(time.time() - ts) > 300:
                abort(401, "stale signature")
            base = f"{ts}.{raw_body.decode('utf-8', errors='ignore')}".encode("utf-8")
            expected = hmac_sha256(WEBHOOK_SECRET, base)
        else:
            expected = hmac_sha256(WEBHOOK_SECRET, raw_body)

        if not hmac.compare_digest(expected, sig_hex):
            abort(401, "bad signature")
    elif not ALLOW_LEGACY_NO_HMAC:
        abort(401, "missing signature")

    # 3) Idempotencia por request-id (si viene)
    req_id = headers.get("X-Request-Id") or headers.get("x-request-id") or ""
    if already_processed(req_id):
        return False, req_id
    return True, req_id

@app.route("/webhook", methods=["POST"])
def webhook():
    # Cuerpo crudo (necesario para HMAC)
    raw = request.get_data(cache=False, as_text=False)

    # Autenticación / verificación
    try:
        should_process, req_id = verify_request(raw, request.headers, request.args)
    except Exception as e:
        # No expongas detalles de autenticación
        app.logger.warning(f"Unauthorized webhook: {e}")
        return ("", 401)

    # Unificar payload (JSON + form + query)
    payload = {}
    if request.is_json:
        payload.update(request.get_json(silent=True) or {})
    payload.update(request.form.to_dict())
    payload.update(request.args.to_dict())

    # Logs higiénicos (sin Authorization ni secretos)
    redacted = {"authorization", "x-api-key"}
    safe_headers = {k: v for k, v in request.headers.items() if k.lower() not in redacted}
    app.logger.info(f"📩 req_id={req_id} Headers={safe_headers}")
    app.logger.info(f"📦 Payload={payload}")

    # Normalizar evento de MP:
    # Nuevo: {"type":"payment","data":{"id":"123"}}
    # Legado: topic=id/resource_id
    ev_type = (payload.get("type") or payload.get("topic") or "").lower()
    data_id = (
        (payload.get("data") or {}).get("id")
        or payload.get("id")
        or payload.get("resource_id")
    )

    # (Opcional) Consultar detalles del pago a la API de MP
    if ev_type == "payment" and data_id and MP_ACCESS_TOKEN:
        try:
            r = requests.get(
                f"https://api.mercadopago.com/v1/payments/{data_id}",
                headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"},
                timeout=10,
            )
            app.logger.info(f"🔎 MP payment {data_id}: status={r.status_code}")
            # app.logger.debug(r.json())  # descomenta si necesitas el JSON completo
        except Exception as e:
            app.logger.error(f"⚠️ Error consultando MP: {e}")

    # Responder rápido SIEMPRE
    return ("", 200)

@app.route("/", methods=["GET"])
def home():
    return "Webhook funcionando 🚀", 200
