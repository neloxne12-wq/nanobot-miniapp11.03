import hashlib
import hmac
import json
import logging
import os
import time
import base64
from urllib.parse import parse_qs, unquote

from aiohttp import web
from database import db, ADMIN_IDS

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
WEB_API_PORT = int(os.getenv("PORT", os.getenv("WEB_API_PORT", "8000")))
MINIAPP_DIR = os.path.dirname(os.path.abspath(__file__))


def validate_init_data(init_data: str, bot_token: str) -> dict | None:
    """Validate Telegram WebApp initData. Returns parsed user dict or None."""
    if not init_data:
        return None
    try:
        parsed = parse_qs(init_data, keep_blank_values=True)
        received_hash = parsed.get("hash", [None])[0]
        if not received_hash:
            return None

        data_check_parts = []
        for key in sorted(parsed.keys()):
            if key == "hash":
                continue
            data_check_parts.append(f"{key}={parsed[key][0]}")
        data_check_string = "\n".join(data_check_parts)

        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(computed_hash, received_hash):
            return None

        auth_date = int(parsed.get("auth_date", ["0"])[0])
        if time.time() - auth_date > 86400:
            return None

        user_json = parsed.get("user", [None])[0]
        if not user_json:
            return None
        return json.loads(unquote(user_json))
    except Exception as e:
        logger.warning(f"initData validation failed: {e}")
        return None


def get_user_from_request(request: web.Request) -> dict | None:
    """Extract and validate user from Authorization header (initData)."""
    auth = request.headers.get("Authorization", "")
    if auth.startswith("tma "):
        init_data = auth[4:]
    else:
        init_data = auth

    user = validate_init_data(init_data, BOT_TOKEN)
    if user:
        return user

    # Fallback: X-Telegram-User-Id header (mini app sends when initData пустой)
    header_uid = request.headers.get("X-Telegram-User-Id")
    if header_uid and str(header_uid).isdigit():
        return {"id": int(header_uid), "first_name": "User"}

    # Fallback: query param
    query_uid = request.query.get("user_id")
    if query_uid and query_uid.isdigit():
        return {"id": int(query_uid), "first_name": "User"}

    return None


def _resolve_user(request, data=None):
    """Resolve user; falls back to body user_id, then admin for testing."""
    user = get_user_from_request(request)
    if user:
        return user
    body_uid = (data or {}).get("user_id") if data else None
    if body_uid and str(body_uid).isdigit() and int(body_uid) > 0:
        return {"id": int(body_uid), "first_name": "User"}
    return {"id": ADMIN_IDS[0] if ADMIN_IDS else 0, "first_name": "TestUser"}


def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Telegram-User-Id",
    }


def json_response(data, status=200):
    return web.json_response(data, status=status, headers=cors_headers())


def error_response(msg, status=400):
    return json_response({"ok": False, "error": msg}, status=status)


async def handle_options(request):
    return web.Response(status=204, headers=cors_headers())


# ── User endpoints ──

async def get_user_info(request):
    user = _resolve_user(request)
    uid = user["id"]
    db.get_or_create_user(uid, user.get("username"), user.get("first_name"), user.get("last_name"))
    info = db.get_user_info(uid)
    return json_response({"ok": True, "user": info})


async def get_user_balance(request):
    user = _resolve_user(request)
    uid = user["id"]
    db.get_or_create_user(uid, user.get("username"), user.get("first_name"), user.get("last_name"))
    info = db.get_user_info(uid)
    return json_response({
        "ok": True,
        "generations_left": info["generations_left"],
        "is_admin": info["is_admin"],
    })


# ── Templates endpoints ──

async def get_templates(request):
    templates = db.get_templates(active_only=True)
    categories = db.get_categories()
    return json_response({"ok": True, "templates": templates, "categories": categories})


async def get_all_templates(request):
    """Admin: all templates including inactive."""
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    templates = db.get_templates(active_only=False)
    categories = db.get_categories()
    return json_response({"ok": True, "templates": templates, "categories": categories})


async def add_template(request):
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    data = await request.json()
    new_id = db.add_template(data)
    return json_response({"ok": True, "id": new_id})


async def update_template(request):
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    tpl_id = int(request.match_info["id"])
    data = await request.json()
    ok = db.update_template(tpl_id, data)
    return json_response({"ok": ok})


async def delete_template(request):
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    tpl_id = int(request.match_info["id"])
    ok = db.delete_template(tpl_id)
    return json_response({"ok": ok})


async def toggle_template(request):
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    tpl_id = int(request.match_info["id"])
    tpl = db.get_template(tpl_id)
    if not tpl:
        return error_response("Not found", 404)
    new_active = 0 if tpl["active"] else 1
    db.update_template(tpl_id, {"active": new_active})
    return json_response({"ok": True, "active": bool(new_active)})


# ── Categories ──

async def get_categories(request):
    cats = db.get_categories()
    return json_response({"ok": True, "categories": cats})


async def add_category(request):
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    data = await request.json()
    cat_id = data.get("id", "")
    label = data.get("label", "")
    emoji = data.get("emoji", "")
    if not cat_id or not label:
        return error_response("id and label required")
    ok = db.add_category(cat_id, label, emoji)
    return json_response({"ok": ok})


async def delete_category(request):
    user = get_user_from_request(request)
    if not user or user["id"] not in ADMIN_IDS:
        return error_response("Forbidden", 403)
    cat_id = request.match_info["id"]
    ok = db.delete_category(cat_id)
    return json_response({"ok": ok})


# ── Generate (via template) ──

async def generate_from_template(request):
    """Start generation from a template. Deducts balance, calls kie.ai API."""
    data = await request.json()
    user = _resolve_user(request, data)
    uid = user["id"]
    db.get_or_create_user(uid, user.get("username"), user.get("first_name"), user.get("last_name"))
    template_id = data.get("template_id")
    image_size = data.get("image_size", "9:16")
    images_b64 = data.get("images", [])

    tpl = db.get_template(template_id) if template_id else None
    prompt = data.get("prompt", "")
    cost = data.get("cost", 1)

    if tpl:
        prompt = tpl["prompt"] or prompt
        cost = tpl["cost"] or 1

    info = db.get_user_info(uid)
    if not info["is_admin"] and info["generations_left"] < cost:
        return error_response("Недостаточно генераций", 402)

    from telegram_bot import (
        generate_image_via_api,
        edit_image_via_api,
        upload_image_to_temporary_host,
        ensure_jpeg_for_api,
    )

    try:
        if images_b64:
            image_bytes_list = []
            for b64 in images_b64[:4]:
                raw = b64.split(",", 1)[-1] if "," in b64 else b64
                img_bytes = base64.b64decode(raw)
                img_bytes = ensure_jpeg_for_api(img_bytes) or img_bytes
                image_bytes_list.append(img_bytes)

            result_bytes = await edit_image_via_api(image_bytes_list, prompt, image_size)
        else:
            result_bytes = await generate_image_via_api(prompt, image_size)

        if not result_bytes:
            return error_response("Генерация не удалась, попробуйте ещё раз", 500)

        gen_type = "template" if tpl else "generate"
        db.use_generation(uid, prompt[:200], gen_type, cost=cost)
        if tpl:
            db.increment_template_uses(template_id)

        result_b64 = base64.b64encode(result_bytes).decode()
        new_info = db.get_user_info(uid)

        # Сохраняем изображение на диск и в историю
        img_id = store_download(result_bytes)
        download_url = f"/api/download/{img_id}"
        gen_name = tpl["name"] if tpl else "Генерация"
        try:
            image_data_uri = f"data:image/jpeg;base64,{result_b64}"
            db.add_to_history(uid, gen_name, prompt[:200], image_size, image_data_uri)
        except Exception as e:
            logger.warning(f"Failed to save history for {uid}: {e}")

        # Push-уведомление в чат
        try:
            from telegram_bot import bot
            from aiogram.types import BufferedInputFile
            caption = f"✅ <b>Генерация готова!</b>\n\n💭 <i>{prompt[:100]}</i>\n\n📂 Откройте мини-приложение → «Мои»"
            await bot.send_photo(
                chat_id=uid,
                photo=BufferedInputFile(result_bytes, filename="generation.jpg"),
                caption=caption,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Failed to send push to {uid}: {e}")

        return json_response({
            "ok": True,
            "image": f"data:image/jpeg;base64,{result_b64}",
            "download_url": download_url,
            "generations_left": new_info["generations_left"],
        })

    except Exception as e:
        logger.error(f"Generation error for user {uid}: {e}")
        error_msg = str(e)
        if "API_ERROR" in error_msg:
            return error_response(error_msg.replace("API_ERROR: ", ""), 500)
        if "TIMEOUT_ERROR" in error_msg:
            return error_response("Превышено время ожидания", 504)
        return error_response("Внутренняя ошибка сервера", 500)


# ── Image storage (disk, survives restarts) ──
import uuid as _uuid

_IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generated_images")
os.makedirs(_IMAGES_DIR, exist_ok=True)


def store_download(image_bytes: bytes) -> str:
    img_id = _uuid.uuid4().hex[:16]
    path = os.path.join(_IMAGES_DIR, img_id + ".jpg")
    with open(path, "wb") as f:
        f.write(image_bytes)
    # Оставляем последние 200 файлов
    try:
        files = sorted(
            [os.path.join(_IMAGES_DIR, n) for n in os.listdir(_IMAGES_DIR) if n.endswith(".jpg")],
            key=os.path.getmtime
        )
        for old in files[:-200]:
            try: os.remove(old)
            except Exception: pass
    except Exception:
        pass
    return img_id


async def download_image(request):
    img_id = request.match_info["id"]
    if not img_id.replace("-", "").replace("_", "").isalnum():
        return error_response("Invalid id", 400)
    path = os.path.join(_IMAGES_DIR, img_id + ".jpg")
    if not os.path.exists(path):
        return error_response("File not found or expired", 404)
    with open(path, "rb") as f:
        data = f.read()
    download = request.query.get("dl") == "1"
    headers = {**cors_headers()}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="nano-banano-{img_id}.jpg"'
    return web.Response(body=data, content_type="image/jpeg", headers=headers)


# ── History ──

async def get_history(request):
    user = _resolve_user(request)
    history = db.get_history(user["id"])
    return json_response({"ok": True, "history": history})


async def delete_history_item(request):
    user = _resolve_user(request)
    item_id = int(request.match_info["id"])
    ok = db.delete_history_item(user["id"], item_id)
    return json_response({"ok": ok})


# ── Payments ──

PLANS = {
    "mini":      {"name": "МИНИ",      "price": 149,  "generations": 5,  "emoji": "🌟"},
    "starter":   {"name": "STARTER",   "price": 249,  "generations": 10, "emoji": "🟢"},
    "pro":       {"name": "PRO",       "price": 599,  "generations": 30, "emoji": "🔵"},
    "unlimited": {"name": "UNLIMITED", "price": 1490, "generations": 90, "emoji": "⭐"},
}


async def request_payment(request):
    """Отправить инвойс YooKassa через бота в чат пользователя."""
    data = await request.json() if request.can_read_body else {}
    user = _resolve_user(request, data)
    uid = user["id"]
    plan = (data.get("plan") or "").strip().lower().replace(" ", "")
    alias = {"мини": "mini", "starter": "starter", "pro": "pro", "unlimited": "unlimited"}
    plan = alias.get(plan, plan)
    if plan not in PLANS:
        return error_response("Тариф не найден", 400)
    plan_info = PLANS[plan]
    try:
        from telegram_bot import bot, PAYMENT_TOKEN
        from aiogram import types as tg_types
        from datetime import datetime as dt
        if not PAYMENT_TOKEN:
            return error_response("Платежи временно недоступны", 503)
        payload = f"{plan}_{uid}_{int(dt.now().timestamp())}"
        await bot.send_invoice(
            chat_id=uid,
            title=f"{plan_info['emoji']} Тариф {plan_info['name']}",
            description=f"🎨 {plan_info['generations']} генераций изображений с помощью AI",
            payload=payload,
            provider_token=PAYMENT_TOKEN,
            currency="RUB",
            prices=[tg_types.LabeledPrice(label=f"Тариф {plan_info['name']}", amount=int(plan_info["price"] * 100))],
            max_tip_amount=0,
            suggested_tip_amounts=[],
            need_name=False, need_phone_number=False, need_email=False,
            need_shipping_address=False, is_flexible=False,
        )
        return json_response({"ok": True, "message": "Счёт отправлен в чат. Проверьте сообщения."})
    except Exception as e:
        logger.error(f"Failed to send invoice to {uid}: {e}")
        return error_response("Ошибка создания счёта", 500)


# ── Shop / payment info ──

async def get_shop_plans(request):
    plans = [
        {"id": "MINI", "name": "МИНИ", "gens": 5, "price": 149, "currency": "RUB"},
        {"id": "STARTER", "name": "STARTER", "gens": 10, "price": 249, "currency": "RUB"},
        {"id": "PRO", "name": "PRO", "gens": 30, "price": 599, "currency": "RUB", "hit": True},
        {"id": "UNLIMITED", "name": "UNLIMITED", "gens": 90, "price": 1490, "currency": "RUB"},
    ]
    return json_response({"ok": True, "plans": plans})


# ── Static file serving for mini app ──

async def serve_miniapp(request):
    """Serve mini app HTML. Tries Index.html in current dir, then nano-banano.html in parent."""
    candidates = [
        os.path.join(MINIAPP_DIR, "Index.html"),
        os.path.join(os.path.dirname(MINIAPP_DIR), "nano-banano.html"),
    ]
    for fp in candidates:
        if os.path.exists(fp):
            return web.FileResponse(fp, headers=cors_headers())
    return error_response("Mini app not found", 404)


def create_app() -> web.Application:
    app = web.Application()

    # CORS preflight
    app.router.add_route("OPTIONS", "/{path:.*}", handle_options)

    # User
    app.router.add_get("/api/user", get_user_info)
    app.router.add_get("/api/balance", get_user_balance)

    # Templates
    app.router.add_get("/api/templates", get_templates)
    app.router.add_get("/api/admin/templates", get_all_templates)
    app.router.add_post("/api/templates", add_template)
    app.router.add_put("/api/templates/{id}", update_template)
    app.router.add_delete("/api/templates/{id}", delete_template)
    app.router.add_post("/api/templates/{id}/toggle", toggle_template)

    # Categories
    app.router.add_get("/api/categories", get_categories)
    app.router.add_post("/api/categories", add_category)
    app.router.add_delete("/api/categories/{id}", delete_category)

    # Generate
    app.router.add_post("/api/generate", generate_from_template)

    # History
    app.router.add_get("/api/history", get_history)
    app.router.add_delete("/api/history/{id}", delete_history_item)

    # Download
    app.router.add_get("/api/download/{id}", download_image)

    # Shop
    app.router.add_get("/api/shop", get_shop_plans)
    app.router.add_post("/api/request-payment", request_payment)

    # Mini app HTML
    app.router.add_get("/", serve_miniapp)
    app.router.add_get("/miniapp", serve_miniapp)

    return app


async def start_web_api():
    """Start the web API server (call from asyncio event loop)."""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEB_API_PORT)
    await site.start()
    logger.info(f"🌐 Web API started on http://0.0.0.0:{WEB_API_PORT}")
    return runner
