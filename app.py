from flask import Flask, request, abort
import os, requests, json, hmac, hashlib, time, threading

app = Flask(__name__)

# === ENV ===
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN", "")          # para consultar /v1/payments/{id}
WEBHOOK_SECRET  = os.getenv("MP_SECRET_KEY", "")            # el de tu captura (obligatorio)
REQUIRE_TOKEN_QS = True                                     # fuerza token ?token=... en la URL
ALLOW_LEGACY_NO_HMAC = False                                # pon True sólo para pruebas locales

# === Idempotencia simple en memoria (Cloud Run instancia única por request; sirve para reintentos cortos) ===
_processed = set()
lock = threading.Lock()

def already_processed(req_id: str) -> bool:
    if not req_id: 
        return False
    with lock:
        if req_id in _processed: 
            return True
        _processed.add(req_id)
        # purga básica
        if len(_processed) > 10000:
            _processed.clear()
        return False

def hmac_sha256(secret: str, data: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()

def parse_signature_header(sig_hdr: str):
    """
    Acepta formatos comunes:
      - 'sha256=<hex>'
      - 't=<ts>, v1=<hex>' (estilo Stripe-like / algunos proveedores)
      - 'ts=<ts>,sha256=<hex>'
    Regresa (ts:int|None, hex:str|None)
    """
    if not sig_hdr:
        return None, None
    sig_hdr = sig_hdr.strip()
    if sig_hdr.startswith("sha256="):
        return None, sig_hdr.split("=", 1)[1]
    parts = {}
    for chunk in sig_hdr.replace(" ", "").split(","):
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            parts[k.lower()] = v
    ts = None
    if "t" in parts:
        try: ts = int(parts["t"])
        except: ts = None
    hexv = parts.get("v1") or parts.get("sha256")
    return ts, hexv

def verify_request(raw_body: bytes, headers, query_args):
    # 0) Token por query obligatorio (defensa simple y efectiva)
    if REQUIRE_TOKEN_QS:
        token = query_args.get("token")
        if not token or token != WEBHOOK_SECRET:
            abort(401, "bad token")

    # 1) HMAC del body si se envía cabecera de firma
    sig_hdr = headers.get("X-Signature") or headers.get("x-signature")
    req_id  = headers.get("X-Request-Id") or headers.get("x-request-id") or ""
    ts, sig_hex = parse_signature_header(sig_hdr)

    if sig_hex:
        # (opcional) rechazar firmas viejas > 5 min si viene ts
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
        # si no viene firma, y no estamos en modo legado, negar
        abort(401, "missing signature")

    # 2) Idempotencia
    if alr
