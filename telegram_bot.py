import asyncio
import logging
import os
from typing import List, Optional, Dict
from collections import defaultdict
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import database
from database import db, ADMIN_IDS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set")

# Nano Banana API configuration
NANO_BANANA_API_URL = os.getenv("NANO_BANANA_API_URL", "https://api.kie.ai")
NANO_BANANA_API_KEY = os.getenv("NANO_BANANA_API_KEY")
if not NANO_BANANA_API_KEY:
    raise ValueError("NANO_BANANA_API_KEY environment variable is not set")

# Payment configuration
USE_TEST_PAYMENTS = os.getenv("USE_TEST_PAYMENTS", "True").lower() == "true"
YOOKASSA_TEST_TOKEN = os.getenv("YOOKASSA_TEST_TOKEN")
YOOKASSA_LIVE_TOKEN = os.getenv("YOOKASSA_LIVE_TOKEN")

# Select appropriate payment token
PAYMENT_TOKEN = YOOKASSA_TEST_TOKEN if USE_TEST_PAYMENTS else YOOKASSA_LIVE_TOKEN

if not PAYMENT_TOKEN:
    logger.warning("⚠️ Payment token not configured - payments will be unavailable")
else:
    mode = "TEST" if USE_TEST_PAYMENTS else "LIVE"
    logger.info(f"💳 Payment system initialized in {mode} mode")

# Channel for +2 gens reward
CHANNEL_USERNAME = "@AIARTpromp"
CHANNEL_URL = "https://t.me/AIARTpromp"

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Storage for media groups
media_groups: Dict[str, List[types.Message]] = defaultdict(list)
media_group_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
media_group_captions: Dict[str, str] = {}  # Store captions for media groups

# FSM States
class GenerateImageState(StatesGroup):
    waiting_for_prompt = State()
    waiting_for_resolution = State()

class EditImageState(StatesGroup):
    waiting_for_images = State()
    waiting_for_prompt = State()
    waiting_for_resolution = State()

class AdminState(StatesGroup):
    search_user = State()
    add_subscription = State()
    add_generations = State()
    broadcast_message = State()
    create_promocode = State()
    create_promo_code = State()
    create_promo_value_custom = State()
    create_promo_max_custom = State()
    create_promo_days_custom = State()
    delete_promocode = State()

class PromocodeState(StatesGroup):
    waiting_for_code = State()

class UpscaleImageState(StatesGroup):
    waiting_for_image = State()
    waiting_for_factor = State()

# Resolution options
RESOLUTIONS = {
    "1:1": "1:1",
    "9:16": "9:16",
    "16:9": "16:9",
    "3:4": "3:4",
    "4:3": "4:3",
    "3:2": "3:2",
    "2:3": "2:3",
    "5:4": "5:4",
    "4:5": "4:5",
    "21:9": "21:9"
}

# Upscale factors and costs (in generations)
UPSCALE_FACTORS = {
    "1": {"name": "1K", "cost": 1},
    "2": {"name": "2K", "cost": 2},
    "4": {"name": "4K", "cost": 4},
    "8": {"name": "8K", "cost": 6}
}

def get_main_menu_keyboard(user_id: int = None) -> InlineKeyboardMarkup:
    """Create main menu keyboard with all options"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🎨 Создать изображение", callback_data="generate_image")
    builder.button(text="✏️ Изменить изображение", callback_data="edit_image")
    builder.button(text="⬆️ Повысить качество", callback_data="upscale_image")
    builder.button(text="💎 Магазин", callback_data="shop")
    builder.button(text="📋 Еще", callback_data="more_menu")
    # Кнопка Шаблоны — открывает мини-приложение
    mini_app_url = os.getenv("MINI_APP_URL")
    if mini_app_url:
        builder.button(text="📄 Шаблоны", web_app=WebAppInfo(url=mini_app_url))
        builder.adjust(2, 2, 1, 1)  # 2+2+1+Шаблоны
    else:
        builder.adjust(2, 2, 1)  # 2+2+1
    
    # Add admin button if user is admin
    if user_id and user_id in ADMIN_IDS:
        builder.button(text="⚙️ Админка", callback_data="admin_panel")
    
    builder.adjust(1)
    return builder.as_markup()

def get_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard with back to main menu button"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🏠 В главное меню", callback_data="back_to_menu")
    return builder.as_markup()

def get_generated_image_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard with upscale and back to menu buttons"""
    builder = InlineKeyboardBuilder()
    builder.button(text="⬆️ Повысить качество", callback_data="upscale_this_image")
    builder.button(text="🏠 В главное меню", callback_data="back_to_menu")
    builder.adjust(1)  # Each button on separate line
    return builder.as_markup()

def get_back_keyboard(back_callback: str) -> InlineKeyboardMarkup:
    """Create keyboard with back button"""
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=back_callback)
    return builder.as_markup()

def get_create_new_image_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard after upscale with action buttons"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🎨 Создать изображение", callback_data="generate_image")
    builder.button(text="✏️ Изменить изображение", callback_data="edit_image")
    builder.button(text="🏠 Главное меню", callback_data="back_to_menu")
    builder.adjust(1)  # Each button on separate line
    return builder.as_markup()

def get_resolution_keyboard(back_callback: str = None, show_all: bool = False, 
                           user_id: int = None, show_save: bool = False) -> InlineKeyboardMarkup:
    """Create resolution selection keyboard with 4 main buttons or all formats"""
    builder = InlineKeyboardBuilder()

    if show_all:
        # Show all additional resolutions
        resolutions = [
            ("4:3", "4:3"), ("3:4", "3:4"), ("3:2", "3:2"),
            ("2:3", "2:3"), ("5:4", "5:4"), ("4:5", "4:5"), 
            ("21:9", "21:9")
        ]
        for text, callback_data in resolutions:
            builder.button(text=text, callback_data=f"resolution_{callback_data}")
        builder.adjust(3)  # 3 buttons per row
    else:
        # Show only 4 main buttons
        main_resolutions = [
            ("16:9", "16:9"),
            ("9:16", "9:16"),
            ("1:1", "1:1"),
            ("📐 Ещё", "show_all_resolutions")
        ]
        for text, callback_data in main_resolutions:
            if callback_data == "show_all_resolutions":
                builder.button(text=text, callback_data=callback_data)
            else:
                builder.button(text=text, callback_data=f"resolution_{callback_data}")
        builder.adjust(2, 2)  # 2x2 grid
    
    # Add back button if provided
    if back_callback:
        builder.row()
        builder.button(text="⬅️ Назад", callback_data=back_callback)

    return builder.as_markup()

def get_admin_menu_keyboard() -> InlineKeyboardMarkup:
    """Create admin panel main menu keyboard"""
    builder = InlineKeyboardBuilder()
    builder.button(text="📊 Статистика", callback_data="admin_stats")
    builder.button(text="👤 Поиск пользователя", callback_data="admin_search")
    builder.button(text="👥 Последние пользователи", callback_data="admin_users")
    builder.button(text="🎨 Последние генерации", callback_data="admin_generations")
    builder.button(text="🎁 Промокоды", callback_data="admin_promocodes")
    builder.button(text="📢 Broadcast", callback_data="admin_broadcast")
    builder.button(text="🏠 Главное меню", callback_data="back_to_menu")
    builder.adjust(2)
    return builder.as_markup()

def get_admin_back_keyboard() -> InlineKeyboardMarkup:
    """Create keyboard with back to admin panel button"""
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ В админ-панель", callback_data="admin_panel")
    return builder.as_markup()

def get_upscale_factor_keyboard(show_back: bool = False) -> InlineKeyboardMarkup:
    """Create upscale factor selection keyboard"""
    builder = InlineKeyboardBuilder()
    
    for factor, info in UPSCALE_FACTORS.items():
        cost = info['cost']
        # Правильное склонение для русского языка
        if cost == 1:
            cost_text = f"{cost} генерация"
        elif 2 <= cost <= 4:
            cost_text = f"{cost} генерации"
        else:
            cost_text = f"{cost} генераций"
        
        builder.button(
            text=f"{info['name']} - {cost_text}",
            callback_data=f"upscale_factor_{factor}"
        )
    
    builder.adjust(2)  # 2 buttons per row
    builder.row()
    
    if show_back:
        builder.button(text="⬅️ Назад", callback_data="back_to_upscale_image")
    
    builder.button(text="🏠 В главное меню", callback_data="back_to_menu")
    return builder.as_markup()

async def check_channel_subscription(user_id: int) -> bool:
    """Check if user is subscribed to channel. BOT MUST BE ADMIN of @AIARTpromp!"""
    try:
        member = await bot.get_chat_member(
            chat_id=CHANNEL_USERNAME,
            user_id=user_id
        )
        ok_statuses = (ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR,
                      ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED)
        return member.status in ok_statuses
    except Exception as e:
        logger.error(f"Channel check failed. Add bot as admin of {CHANNEL_USERNAME}: {e}")
        return False

async def should_show_channel_notification(user_id: int) -> bool:
    """Show channel notification if: not subscribed yet (all users: paid and free)"""
    if await check_channel_subscription(user_id):
        return False
    return True

async def should_show_channel_after_generation(user_id: int) -> bool:
    """CTA показывается только ДО генераций, после — не показываем"""
    return False

def get_channel_keyboard(from_generate: bool = False, from_upscale: bool = False, 
                         from_edit: bool = False, no_generations: bool = False) -> InlineKeyboardMarkup:
    """Keyboard for channel subscription CTA. When no_generations: 3rd btn = Магазин."""
    if from_upscale:
        check_cb, skip_cb = "check_channel_sub_upscale", "skip_channel_sub_upscale"
    elif from_edit:
        check_cb, skip_cb = "check_channel_sub_edit", "skip_channel_sub_edit"
    elif from_generate:
        check_cb, skip_cb = "check_channel_sub_gen", "skip_channel_sub_gen"
    else:
        check_cb, skip_cb = "check_channel_sub", "skip_channel_sub"
    builder = InlineKeyboardBuilder()
    builder.button(text="📢 Подписаться на канал", url=CHANNEL_URL)
    builder.button(text="✅ Я уже подписан", callback_data=check_cb)
    if no_generations:
        builder.button(text="💎 Магазин", callback_data="shop")
    else:
        builder.button(text="➡️ Подписаться позже", callback_data=skip_cb)
    builder.adjust(1)
    return builder.as_markup()

def get_user_manage_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Create keyboard for user management"""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Выдать подписку", callback_data=f"admin_give_sub_{user_id}")
    builder.button(text="🎁 Добавить генерации", callback_data=f"admin_add_gen_{user_id}")
    builder.button(text="❌ Отменить подписку", callback_data=f"admin_cancel_sub_{user_id}")
    builder.button(text="⬅️ Назад", callback_data="admin_panel")
    builder.adjust(1)
    return builder.as_markup()

def get_subscription_plans_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Create keyboard with subscription plans for admin"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🌟 МИНИ (5 ген)", callback_data=f"admin_sub_mini_{user_id}")
    builder.button(text="🟢 STARTER (10 ген)", callback_data=f"admin_sub_starter_{user_id}")
    builder.button(text="🔵 PRO (30 ген)", callback_data=f"admin_sub_pro_{user_id}")
    builder.button(text="⭐ UNLIMITED (90 ген)", callback_data=f"admin_sub_unlimited_{user_id}")
    builder.button(text="⬅️ Назад", callback_data=f"admin_user_{user_id}")
    builder.adjust(1)
    return builder.as_markup()

async def generate_image_via_api(prompt: str, resolution: str) -> Optional[bytes]:
    """Generate image using Nano Banana API"""
    try:
        connector = aiohttp.TCPConnector(limit=10, force_close=False, enable_cleanup_closed=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            headers = {
                "Authorization": f"Bearer {NANO_BANANA_API_KEY}",
                "Content-Type": "application/json"
            }

            # Step 1: Create task
            payload = {
                "model": "google/nano-banana",
                "input": {
                    "prompt": prompt,
                    "output_format": "jpeg",
                    "image_size": resolution
                }
            }

            async with session.post(
                f"{NANO_BANANA_API_URL}/api/v1/jobs/createTask",
                headers=headers,
                json=payload
            ) as response:
                if response.status != 200:
                    logger.error(f"API error creating task: {response.status} - {await response.text()}")
                    return None
                
                result = await response.json()
                if result.get("code") != 200:
                    logger.error(f"API error: {result.get('msg')}")
                    return None
                
                task_id = result.get("data", {}).get("taskId")
                if not task_id:
                    logger.error("No taskId in response")
                    return None

            logger.info(f"Task created: {task_id}")
            
            import time
            start_time = time.time()

            # Step 2: Poll for results with strict timeout
            max_attempts = 40  # Меньше попыток, но с большим интервалом
            for attempt in range(max_attempts):
                # Проверяем сразу первый раз, потом с задержкой
                if attempt > 0:
                    # Каждые 6 секунд проверяем статус (чтобы не перегружать)
                    await asyncio.sleep(6.0)
                
                # Проверяем таймаут перед каждым запросом
                elapsed = time.time() - start_time
                if elapsed > 180:  # Строгий таймаут 180 секунд (3 минуты)
                    logger.error(f"❌ Task timeout after {elapsed:.1f}s (strict 180s limit)")
                    return None

                try:
                    async with session.get(
                        f"{NANO_BANANA_API_URL}/api/v1/jobs/recordInfo?taskId={task_id}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status != 200:
                            logger.error(f"API error checking status: {response.status}")
                            continue
                        
                        result = await response.json()
                        if result.get("code") != 200:
                            logger.error(f"API error: {result.get('msg')}")
                            continue

                        data = result.get("data", {})
                        state = data.get("state")
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Timeout checking status, retrying... (attempt {attempt + 1})")
                    continue
                except Exception as e:
                    logger.warning(f"⚠️ Error checking status: {e}, retrying...")
                    continue

                if state == "success":
                    elapsed = time.time() - start_time
                    logger.info(f"✅ Generation completed in {elapsed:.2f}s")
                    
                    # Extract image URL from resultJson
                    import json
                    result_json = json.loads(data.get("resultJson", "{}"))
                    result_urls = result_json.get("resultUrls", [])
                    
                    if not result_urls:
                        logger.error("No result URLs in response")
                        return None
                    
                    image_url = result_urls[0]
                    logger.info(f"Image URL: {image_url}")

                    # Step 3: Download image
                    async with session.get(image_url) as img_response:
                        if img_response.status == 200:
                            return await img_response.read()
                        else:
                            logger.error(f"Failed to download image: {img_response.status}")
                            return None

                elif state == "fail":
                    fail_msg = data.get("failMsg", "Unknown error")
                    logger.error(f"Task failed: {fail_msg}")
                    # Возвращаем специальный маркер для ошибки API
                    raise Exception(f"API_ERROR: {fail_msg}")
                
                # Still waiting
                elapsed = time.time() - start_time
                logger.info(f"⏳ Task status: {state} | Attempt {attempt + 1}/{max_attempts} | Elapsed: {elapsed:.1f}s")

            elapsed = time.time() - start_time
            logger.error(f"❌ Task timeout after {elapsed:.1f}s")
            raise Exception("TIMEOUT_ERROR: Превышено время ожидания генерации")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error generating image: {error_msg}")
        # Пробрасываем исключение дальше для обработки
        raise

def get_telegram_file_url(file_path: str) -> str:
    """Get direct Telegram file URL"""
    return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"

def ensure_jpeg_for_api(image_bytes: bytes, max_side: int = 4096) -> Optional[bytes]:
    """Convert image to valid JPEG for kie.ai API. Fixes 'Image format error'."""
    img = None
    try:
        from PIL import Image, ImageOps
        import io
        img = Image.open(io.BytesIO(image_bytes)).copy()
        img = ImageOps.exif_transpose(img)
        # Resize if too large (API: longest_side × factor ≤ 20000)
        w, h = img.size
        if max(w, h) > max_side:
            ratio = max_side / max(w, h)
            new_size = (int(w * ratio), int(h * ratio))
            try:
                resample = Image.Resampling.LANCZOS
            except AttributeError:
                resample = Image.LANCZOS
            img = img.resize(new_size, resample)
        # Convert to RGB
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            if img.mode in ('RGBA', 'LA'):
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else img.split()[1])
            else:
                background.paste(img)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        output = io.BytesIO()
        img.save(output, format='JPEG', quality=92)
        result = output.getvalue()
        logger.info(f"Image converted to JPEG: {len(image_bytes)} -> {len(result)} bytes")
        return result
    except Exception as e:
        logger.warning(f"Could not convert image to JPEG: {e}, using original")
        return image_bytes
    finally:
        if img:
            img.close()

async def upload_image_to_temporary_host(image_data: bytes) -> Optional[str]:
    """Upload image to temporary hosting service and get URL"""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            # 1) tmpfiles.org — kie.ai лучше принимает (catbox даёт "Image format error")
            try:
                data = aiohttp.FormData()
                data.add_field('file', image_data, filename='image.jpg', content_type='image/jpeg')
                async with session.post(
                    "https://tmpfiles.org/api/v1/upload",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('status') == 'success':
                            url = result.get('data', {}).get('url', '')
                            if url and 'tmpfiles.org/' in url:
                                url = url.replace('tmpfiles.org/', 'tmpfiles.org/dl/')
                            if url:
                                logger.info(f"Image uploaded to tmpfiles.org: {url[:60]}...")
                                return url
            except Exception as e:
                logger.warning(f"tmpfiles.org failed: {e}")
            # 3) freeimage.host — base64, надёжный Content-Type
            try:
                import base64
                image_base64 = base64.b64encode(image_data).decode('utf-8')
                data = aiohttp.FormData()
                data.add_field('source', image_base64)
                data.add_field('type', 'base64')
                data.add_field('action', 'upload')
                async with session.post(
                    "https://freeimage.host/api/1/upload?key=6d207e02198a847aa98d0a2a901485a5",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('status_code') == 200:
                            url = result.get('image', {}).get('url')
                            if url:
                                logger.info(f"Image uploaded to freeimage.host: {url}")
                                return url
            except Exception as e:
                logger.warning(f"freeimage.host failed: {e}")
            # 3) catbox.moe — запасной
            try:
                data = aiohttp.FormData()
                data.add_field('reqtype', 'fileupload')
                data.add_field('fileToUpload', image_data, filename='image.jpg', content_type='image/jpeg')
                async with session.post(
                    "https://catbox.moe/user/api.php",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status == 200:
                        url = (await response.text()).strip()
                        if url and url.startswith('http'):
                            logger.info(f"Image uploaded to catbox.moe: {url[:60]}...")
                            return url
            except Exception as e:
                logger.warning(f"catbox.moe failed: {e}")

            logger.error("All image hosting services failed")
            return None
    except Exception as e:
        logger.error(f"Error uploading to temporary host: {e}")
        return None

async def upscale_image_via_api(image_url: str, upscale_factor: str) -> Optional[bytes]:
    """Upscale image using Nano Banana Upscale API"""
    try:
        connector = aiohttp.TCPConnector(limit=10, force_close=False, enable_cleanup_closed=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            headers = {
                "Authorization": f"Bearer {NANO_BANANA_API_KEY}",
                "Content-Type": "application/json"
            }

            # Step 1: Create upscale task
            payload = {
                "model": "topaz/image-upscale",
                "input": {
                    "image_url": image_url,
                    "upscale_factor": upscale_factor
                }
            }

            async with session.post(
                f"{NANO_BANANA_API_URL}/api/v1/jobs/createTask",
                headers=headers,
                json=payload
            ) as response:
                if response.status != 200:
                    err_text = await response.text()
                    logger.error(f"Upscale API error creating task: {response.status} - {err_text}")
                    return None
                
                result = await response.json()
                if result.get("code") != 200:
                    msg = result.get("msg", str(result))
                    logger.error(f"Upscale API error (factor={upscale_factor}): code={result.get('code')} msg={msg}")
                    return None
                
                task_id = result.get("data", {}).get("taskId")
                if not task_id:
                    logger.error("No taskId in upscale response")
                    return None

            logger.info(f"Upscale task created: {task_id}")
            
            import time
            start_time = time.time()

            # Step 2: Poll for results with strict timeout
            max_attempts = 40  # Меньше попыток, но с большим интервалом
            for attempt in range(max_attempts):
                if attempt > 0:
                    # Каждые 6 секунд проверяем статус (чтобы не перегружать)
                    await asyncio.sleep(6.0)
                
                # Проверяем таймаут перед каждым запросом
                elapsed = time.time() - start_time
                if elapsed > 180:  # Строгий таймаут 180 секунд (3 минуты)
                    logger.error(f"❌ Upscale timeout after {elapsed:.1f}s (strict 180s limit)")
                    return None

                try:
                    async with session.get(
                        f"{NANO_BANANA_API_URL}/api/v1/jobs/recordInfo?taskId={task_id}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status != 200:
                            logger.error(f"Upscale API error checking status: {response.status}")
                            continue
                        
                        result = await response.json()
                        if result.get("code") != 200:
                            logger.error(f"Upscale API error: {result.get('msg')}")
                            continue

                        data = result.get("data", {})
                        state = data.get("state")
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Upscale timeout checking status, retrying... (attempt {attempt + 1})")
                    continue
                except Exception as e:
                    logger.warning(f"⚠️ Upscale error checking status: {e}, retrying...")
                    continue

                if state == "success":
                    elapsed = time.time() - start_time
                    logger.info(f"✅ Upscale completed in {elapsed:.2f}s")
                    
                    # Extract image URL from resultJson
                    import json
                    result_json = json.loads(data.get("resultJson", "{}"))
                    result_urls = result_json.get("resultUrls", [])
                    
                    if not result_urls:
                        logger.error("No result URLs in upscale response")
                        return None
                    
                    image_url = result_urls[0]
                    logger.info(f"Upscaled image URL: {image_url}")

                    # Step 3: Download image
                    async with session.get(image_url) as img_response:
                        if img_response.status == 200:
                            return await img_response.read()
                        else:
                            logger.error(f"Failed to download upscaled image: {img_response.status}")
                            return None

                elif state == "fail":
                    fail_msg = data.get("failMsg", data.get("fail_msg", "Unknown error"))
                    logger.error(f"Upscale task failed (factor={upscale_factor}): {fail_msg}")
                    return None
                
                # Still waiting
                elapsed = time.time() - start_time
                logger.info(f"⏳ Upscale status: {state} | Attempt {attempt + 1}/{max_attempts} | Elapsed: {elapsed:.1f}s")

            logger.error(f"❌ Upscale timeout after {time.time() - start_time:.1f}s")
            return None

    except Exception as e:
        logger.error(f"Error upscaling image: {e}")
        return None

async def edit_image_via_api(images: List[bytes], prompt: str, resolution: str, telegram_urls: List[str] = None) -> Optional[bytes]:
    """Edit images using Nano Banana Edit API. API не умеет качать по Telegram URL — всегда заливаем на публичный хостинг."""
    try:
        image_urls = []
        for image_data in images[:10]:
            url = await upload_image_to_temporary_host(image_data)
            if url:
                image_urls.append(url)
        
        if not image_urls:
            logger.error("Failed to get any image URLs")
            return None
        
        logger.info(f"Using {len(image_urls)} public URLs for editing")
        
        connector = aiohttp.TCPConnector(limit=10, force_close=False, enable_cleanup_closed=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            headers = {
                "Authorization": f"Bearer {NANO_BANANA_API_KEY}",
                "Content-Type": "application/json"
            }

            # Step 1: Create task with nano-banana-edit model
            payload = {
                "model": "google/nano-banana-edit",
                "input": {
                    "prompt": prompt,
                    "image_urls": image_urls,
                    "output_format": "jpeg",
                    "image_size": resolution
                }
            }

            async with session.post(
                f"{NANO_BANANA_API_URL}/api/v1/jobs/createTask",
                headers=headers,
                json=payload
            ) as response:
                if response.status != 200:
                    logger.error(f"API error creating task: {response.status} - {await response.text()}")
                    return None
                
                result = await response.json()
                if result.get("code") != 200:
                    logger.error(f"API error: {result.get('msg')}")
                    return None
                
                task_id = result.get("data", {}).get("taskId")
                if not task_id:
                    logger.error("No taskId in response")
                    return None

            logger.info(f"Edit task created: {task_id}")
            
            import time
            start_time = time.time()

            # Step 2: Poll for results with strict timeout
            max_attempts = 40  # Увеличено, как у генерации
            for attempt in range(max_attempts):
                # Проверяем сразу первый раз, потом с задержкой
                if attempt > 0:
                    # Каждые 6 секунд проверяем статус (чтобы не перегружать)
                    await asyncio.sleep(6.0)
                
                # Проверяем таймаут перед каждым запросом
                elapsed = time.time() - start_time
                if elapsed > 180:  # Строгий таймаут 180 секунд (как у генерации)
                    logger.error(f"❌ Edit timeout after {elapsed:.1f}s (strict 180s limit)")
                    return None

                try:
                    async with session.get(
                        f"{NANO_BANANA_API_URL}/api/v1/jobs/recordInfo?taskId={task_id}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        if response.status != 200:
                            logger.error(f"API error checking status: {response.status}")
                            continue
                        
                        result = await response.json()
                        if result.get("code") != 200:
                            logger.error(f"API error: {result.get('msg')}")
                            continue
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ Edit timeout checking status, retrying... (attempt {attempt + 1})")
                    continue
                except Exception as e:
                    logger.warning(f"⚠️ Edit error checking status: {e}, retrying...")
                    continue

                data = result.get("data", {})
                state = data.get("state")

                if state == "success":
                    elapsed = time.time() - start_time
                    logger.info(f"✅ Edit completed in {elapsed:.2f}s")
                    
                    import json
                    rj = data.get("resultJson") or "{}"
                    result_json = rj if isinstance(rj, dict) else json.loads(rj)
                    result_urls = result_json.get("resultUrls", result_json.get("result_urls", []))
                    
                    if not result_urls:
                        logger.error("No result URLs in response")
                        return None
                    
                    image_url = result_urls[0]
                    logger.info(f"Image URL: {image_url}")

                    # Download edited image with timeout and retry
                    try:
                        logger.info(f"📥 Downloading edited image from {image_url[:50]}...")
                        async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=60)) as img_response:
                            if img_response.status == 200:
                                image_bytes = await img_response.read()
                                logger.info(f"✅ Downloaded {len(image_bytes)} bytes")
                                return image_bytes
                            else:
                                logger.error(f"❌ Failed to download image: HTTP {img_response.status}")
                                return None
                    except asyncio.TimeoutError:
                        logger.error(f"❌ Timeout downloading image from {image_url[:50]}")
                        return None
                    except Exception as e:
                        logger.error(f"❌ Error downloading image: {e}")
                        return None

                elif state == "fail":
                    fail_msg = data.get("failMsg", "Unknown error")
                    logger.error(f"Task failed: {fail_msg}")
                    # Возвращаем специальный маркер для ошибки API
                    raise Exception(f"API_ERROR: {fail_msg}")
                
                # Still waiting
                elapsed = time.time() - start_time
                logger.info(f"⏳ Edit status: {state} | Attempt {attempt + 1}/{max_attempts} | Elapsed: {elapsed:.1f}s")

            logger.error(f"❌ Edit timeout after {time.time() - start_time:.1f}s")
            raise Exception("TIMEOUT_ERROR: Превышено время ожидания редактирования")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error editing image: {error_msg}")
        # Пробрасываем исключение дальше для обработки
        raise

@dp.message(CommandStart(deep_link=True))
async def start_command_with_ref(message: types.Message):
    """Handle /start command with referral link"""
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "пользователь"
    username = message.from_user.username
    
    # Get deep link argument
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].replace("ref_", ""))
            # Check if this is a new user (first time)
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            is_new_user = cursor.fetchone() is None
            conn.close()
            
            if is_new_user and referrer_id != user_id:
                # Add referral relationship
                success, msg = db.add_referral(referrer_id, user_id)
                if success:
                    logger.info(f"Referral added: {referrer_id} -> {user_id}")
        except Exception as e:
            logger.error(f"Error processing referral: {e}")
    
    # Continue with normal start
    await start_command_normal(message)

@dp.message(CommandStart())
async def start_command_normal(message: types.Message):
    """Handle /start command"""
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "пользователь"
    username = message.from_user.username
    
    # Check if user is new
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    is_new_user = cursor.fetchone() is None
    conn.close()
    
    # Get or create user in database
    db.get_or_create_user(user_id, username, user_name)
    
    # Show onboarding for new users
    if is_new_user:
        await show_onboarding(message)
        return
    
    # Regular welcome for existing users
    await show_main_menu(message)

async def show_onboarding(message: types.Message):
    """Show onboarding guide for new users"""
    user_name = message.from_user.first_name or "друг"
    
    text = (
        f"👋 Привет, <b>{user_name}</b>!\n\n"
        "🎨 <b>Nano Banana Bot</b> — твой AI-художник\n\n"
        "🎁 Вам начислено <b>2 Welcome-генерации</b> для старта!\n\n"
        "✨ <b>Что умею:</b> создавать изображения по описанию, редактировать фото, разные форматы (16:9, 9:16, 1:1)\n\n"
        "📝 <b>Как описывать:</b> объект + детали + окружение + освещение. Например: <i>«Уютное кафе, вечерний свет, деревянные столы, реалистичный стиль»</i>\n\n"
        "⚡ Генерация: 5-15 сек\n\n"
        "Готов начать?"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🎨 Создать первое изображение", callback_data="generate_image")
    builder.button(text="📋 Главное меню", callback_data="back_to_menu")
    builder.adjust(1)
    
    await message.answer(text, reply_markup=builder.as_markup())

async def show_main_menu(message: types.Message):
    """Show main menu for existing users"""
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "пользователь"
    
    # Get user info from database
    user_info = db.get_user_info(user_id)
    
    # Format status
    status = user_info["status"]
    if user_info["is_admin"]:
        status_emoji = "👑"
        generations_text = "∞ / ∞"
    else:
        status_emoji = "📊"
        left = user_info["generations_left"]
        limit = user_info["generations_limit"]
        if left == 0:
            status = "—"
            generations_text = "0"
        else:
            generations_text = f"{left} / {limit}"
    
    welcome_text = (
        f"👋 Привет, <b>{user_name}</b>!\n\n"
        f"{status_emoji} <b>Баланс</b>\n"
        f"├ Статус: <b>{status}</b>\n"
        f"└ Генераций: <b>{generations_text}</b>\n\n"
        "🎨 <b>Nano Banana Bot</b>\n"
        "Генерация и редактирование изображений с AI\n\n"
    )
    
    if not user_info["is_admin"] and user_info["generations_left"] == 0:
        welcome_text += "💡 <i>Купите подписку для генераций</i>\n\n"
    
    welcome_text += "Выберите действие:"

    await message.answer(welcome_text, reply_markup=get_main_menu_keyboard(user_id))

@dp.callback_query(F.data == "generate_image")
async def generate_image_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle generate image button click"""
    await callback.answer()
    
    user_id = callback.from_user.id
    
    # Check generations first (for all users — paid or free)
    can_generate, msg = db.can_generate(user_id)
    if not can_generate:
        if await should_show_channel_notification(user_id):
            await callback.message.edit_text(
                "⛔ <b>У вас нет доступных генераций</b>\n\n"
                "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                "Там мы делимся лучшими промптами, примерами работ и новостями.",
                reply_markup=get_channel_keyboard(no_generations=True)
            )
        else:
            await callback.message.edit_text(
                f"⛔ <b>У вас нет доступных генераций</b>\n\n{msg}\n\n"
                "💎 Перейдите в магазин, чтобы купить подписку",
                reply_markup=get_main_menu_keyboard(user_id)
            )
        return
    
    # If channel notification needed — show CTA first, then prompt
    if await should_show_channel_notification(user_id):
        await state.set_state(GenerateImageState.waiting_for_prompt)
        await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
        await callback.message.edit_text(
            "🎁 <b>Вы можете получить дополнительную 1 генерацию за подписку на канал</b>\n\n"
            "Там мы делимся лучшими промптами, примерами работ и новостями.",
            reply_markup=get_channel_keyboard(from_generate=True)
        )
        return
    
    # Otherwise — go straight to prompt
    await state.clear()
    await state.set_state(GenerateImageState.waiting_for_prompt)
    text = "💬 Введите описание изображения:\n\n<i>Например: 'Уютное кафе с большими окнами, мягкое вечернее освещение'</i>"
    await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())
    await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)

@dp.message(GenerateImageState.waiting_for_prompt, F.photo)
async def process_generation_photo_with_caption(message: types.Message, state: FSMContext):
    """Handle photo with caption in generate mode - use caption as prompt"""
    caption = message.caption.strip() if message.caption else ""
    
    if not caption:
        await message.answer(
            "💡 <b>Подсказка:</b>\n"
            "Отправьте текстовое описание или фото с подписью.\n"
            "Подпись будет использована как описание для генерации.",
            reply_markup=get_back_to_menu_keyboard()
        )
        return
    
    # Use caption as prompt
    await state.update_data(prompt=caption)
    await state.set_state(GenerateImageState.waiting_for_resolution)
    
    user_id = message.from_user.id
    
    text = (
        f"✅ Описание: <b>{caption}</b>\n\n"
        "📐 Выберите соотношение сторон:"
    )
    
    await message.answer(text, reply_markup=get_resolution_keyboard("back_to_prompt", user_id=user_id))

@dp.message(GenerateImageState.waiting_for_prompt)
async def process_generation_prompt(message: types.Message, state: FSMContext):
    """Process the prompt for image generation"""
    prompt = message.text.strip() if message.text else ""

    if not prompt:
        # Delete user message
        try:
            await message.delete()
        except:
            pass
        
        # Get bot message from state and edit it
        data = await state.get_data()
        bot_message_id = data.get("bot_message_id")
        chat_id = data.get("chat_id")
        
        if bot_message_id and chat_id:
            await bot.edit_message_text(
                "❌ Пожалуйста, введите описание изображения.",
                chat_id=chat_id,
                message_id=bot_message_id,
                reply_markup=get_back_to_menu_keyboard()
            )
        return

    # Delete user message
    try:
        await message.delete()
    except:
        pass

    # Store the prompt
    await state.update_data(prompt=prompt)
    await state.set_state(GenerateImageState.waiting_for_resolution)

    user_id = message.from_user.id
    
    text = (
        f"✅ Описание: <b>{prompt}</b>\n\n"
        "📐 Выберите соотношение сторон:"
    )

    # Edit bot message
    data = await state.get_data()
    bot_message_id = data.get("bot_message_id")
    chat_id = data.get("chat_id")
    
    if bot_message_id and chat_id:
        await bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=bot_message_id,
            reply_markup=get_resolution_keyboard("back_to_prompt", user_id=user_id),
            parse_mode=ParseMode.HTML
        )

@dp.callback_query(GenerateImageState.waiting_for_resolution, F.data.startswith("resolution_"))
async def process_generation_resolution(callback: types.CallbackQuery, state: FSMContext):
    """Process resolution selection for image generation"""
    user_id = callback.from_user.id
    
    # Check if user can generate
    can_generate, msg = db.can_generate(user_id)
    if not can_generate:
        await callback.answer("⛔ Нет генераций", show_alert=True)
        await state.clear()
        # Show channel CTA if applicable (instead of shop)
        if await should_show_channel_notification(user_id):
            await callback.message.edit_text(
                "⛔ <b>У вас нет доступных генераций</b>\n\n"
                "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                "Там мы делимся лучшими промптами, примерами работ и новостями.",
                reply_markup=get_channel_keyboard(no_generations=True)
            )
        else:
            await callback.message.edit_text(
                f"⛔ <b>У вас нет доступных генераций</b>\n\n{msg}\n\n"
                "💎 Перейдите в магазин, чтобы купить подписку",
                reply_markup=get_main_menu_keyboard(user_id)
            )
        return
    
    resolution_key = callback.data.replace("resolution_", "")
    resolution = RESOLUTIONS.get(resolution_key, "16:9")
    
    # Auto-save user's preferred resolution
    db.set_user_preferred_resolution(user_id, resolution)

    # Get stored data
    data = await state.get_data()
    prompt = data.get("prompt")

    # Clear state
    await state.clear()

    # Answer callback immediately and update message simultaneously
    await callback.answer("⏳ Генерация началась...")
    progress_message = await callback.message.edit_text(
        "🎨 <b>Генерация началась...</b>\n\n"
        "⏳ Создаю изображение, подождите немного..."
    )

    # Record generation usage
    db.use_generation(user_id, prompt, "generate")
    
    # Check and claim referral reward for referrer (if this is referred user's first generation)
    try:
        referrer_id = db.get_referrer_id(user_id)
        if referrer_id:
            # Claim reward (will only work once per referral)
            if db.claim_referral_reward(referrer_id, user_id, reward_generations=2):
                # Notify referrer
                try:
                    await bot.send_message(
                        referrer_id,
                        "🎉 <b>Реферальная награда!</b>\n\n"
                        f"Ваш друг сделал первую генерацию!\n"
                        f"Вы получили <b>1 генерацию</b> 🎁"
                    )
                except Exception:
                    pass  # Referrer might have blocked the bot
    except Exception as e:
        logger.error(f"Error processing referral reward: {e}")

    # Generate image with progress updates
    import time
    start_gen = time.time()
    
    # Create background task for progress updates
    async def update_progress():
        dots = 0
        while True:
            await asyncio.sleep(3)
            dots = (dots + 1) % 4
            elapsed = int(time.time() - start_gen)
            try:
                prompt_text = f"{prompt[:50]}..." if len(prompt) > 50 else prompt
                await progress_message.edit_text(
                    f"🎨 <b>Генерация изображения</b>{'.' * (dots + 1)}\n\n"
                    f"⏳ Прошло: {elapsed} сек\n"
                    f"💭 Запрос: <i>{prompt_text}</i>"
                )
            except Exception as e:
                logger.debug(f"Progress update error: {e}")
                pass
    
    progress_task = asyncio.create_task(update_progress())
    
    error_message = None
    try:
        # Строгий таймаут 180 секунд для генерации (3 минуты для медленного API)
        image_data = await asyncio.wait_for(
            generate_image_via_api(prompt, resolution),
            timeout=180.0
        )
    except asyncio.TimeoutError:
        logger.error("❌ Generation timeout after 180 seconds")
        image_data = None
        error_message = "TIMEOUT"
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Generation error: {error_msg}")
        image_data = None
        if "API_ERROR:" in error_msg:
            error_message = error_msg.replace("API_ERROR: ", "")
        elif "TIMEOUT_ERROR:" in error_msg:
            error_message = "TIMEOUT"
        else:
            error_message = "UNKNOWN"
    finally:
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

    if image_data:
        # Send the generated image
        sent_message = await callback.message.answer_photo(
            photo=types.BufferedInputFile(image_data, filename="generated_image.jpg"),
            caption="Вот ваше изображение:",
            reply_markup=get_generated_image_keyboard()
        )
        # Save file_id for potential upscale (both in state and database)
        file_id = sent_message.photo[-1].file_id
        await state.update_data(last_generated_image_file_id=file_id)
        db.save_last_generated_image(user_id, file_id)
        
        # Show remaining generations and optional channel CTA
        user_info = db.get_user_info(user_id)
        remaining = user_info.get("generations_left", 0)
        status_text = f"💎 Осталось генераций: {remaining}"
        if await should_show_channel_after_generation(user_id):
            await callback.message.answer(
                status_text + "\n\n🎁 Подпишись на канал и получи ещё +1 генерацию!",
                reply_markup=get_channel_keyboard()
            )
        else:
            await callback.message.answer(status_text)
    else:
        # Формируем сообщение об ошибке в зависимости от типа
        if error_message and error_message != "TIMEOUT" and error_message != "UNKNOWN":
            # Конкретная ошибка от API
            error_text = (
                f"❌ <b>Ошибка при генерации изображения</b>\n\n"
                f"<b>Причина:</b> {error_message}\n\n"
                f"💡 Попробуйте:\n"
                f"• Изменить описание запроса\n"
                f"• Упростить промпт\n"
                f"• Попробовать позже"
            )
        elif error_message == "TIMEOUT":
            error_text = (
                "⏱️ <b>Превышено время ожидания</b>\n\n"
                "Генерация заняла слишком много времени (>3 мин).\n\n"
                "💡 Попробуйте:\n"
                "• Упростить описание\n"
                "• Попробовать еще раз\n"
                "• API сервер может быть перегружен"
            )
        else:
            # Общая ошибка
            error_text = (
                "❌ Произошла ошибка при генерации изображения.\n\n"
                "<b>Возможные причины:</b>\n"
                "• Ваш запрос нарушает политику использования AI\n"
                "• Проблемы с API сервером\n"
                "• Превышен лимит запросов\n\n"
                "💡 Попробуйте:\n"
                "• Изменить описание\n"
                "• Упростить промпт\n"
                "• Попробовать позже"
            )
        
        await callback.message.answer(
            error_text,
            reply_markup=get_main_menu_keyboard(user_id)
        )

@dp.callback_query(F.data == "check_channel_sub")
async def process_channel_check(callback: types.CallbackQuery):
    """Handle channel subscription check"""
    user_id = callback.from_user.id
    
    is_subscribed = await check_channel_subscription(user_id)
    
    if is_subscribed:
        already_claimed = db.check_channel_reward_claimed(user_id)
        
        if already_claimed:
            await callback.answer("✅ Вы уже получали награду за подписку!", show_alert=True)
        else:
            success, reason = db.claim_channel_reward(user_id)
            if success:
                await callback.answer("🎉 +1 генерацию за подписку!", show_alert=True)
                await callback.message.edit_text(
                    "✅ Спасибо за подписку! Вам начислено +1 генерацию!",
                    reply_markup=get_main_menu_keyboard(user_id)
                )
            else:
                await callback.answer("❌ Ошибка при начислении награды", show_alert=True)
    else:
        await callback.answer(
            "❌ Вы ещё не подписались на канал.\nПодпишитесь и нажмите «Проверить подписку» снова.",
            show_alert=True
        )

@dp.callback_query(F.data == "skip_channel_sub")
async def process_skip_channel(callback: types.CallbackQuery):
    """Skip channel subscription"""
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.answer()

async def _show_prompt_for_generation(callback: types.CallbackQuery, state: FSMContext):
    """Show prompt input after channel step (used from _gen handlers)"""
    text = "💬 Введите описание изображения:\n\n<i>Например: 'Уютное кафе с большими окнами, мягкое вечернее освещение'</i>"
    await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "check_channel_sub_gen")
async def process_channel_check_from_generate(callback: types.CallbackQuery, state: FSMContext):
    """Channel check when coming from 'Create first image' — then show prompt"""
    user_id = callback.from_user.id
    is_subscribed = await check_channel_subscription(user_id)
    
    if is_subscribed:
        already_claimed = db.check_channel_reward_claimed(user_id)
        if already_claimed:
            await callback.answer("✅ Вы уже получали награду!", show_alert=True)
        else:
            success, reason = db.claim_channel_reward(user_id)
            if success:
                await callback.answer("🎉 +1 генерацию!", show_alert=True)
            else:
                await callback.answer("❌ Ошибка начисления", show_alert=True)
    else:
        await callback.answer("❌ Подпишитесь на канал и нажмите снова", show_alert=True)
        return
    
    await state.set_state(GenerateImageState.waiting_for_prompt)
    await _show_prompt_for_generation(callback, state)
    await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)

@dp.callback_query(F.data == "skip_channel_sub_gen")
async def process_skip_channel_from_generate(callback: types.CallbackQuery, state: FSMContext):
    """Skip channel from generate flow — show prompt"""
    await callback.answer()
    await state.set_state(GenerateImageState.waiting_for_prompt)
    await _show_prompt_for_generation(callback, state)
    await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)

UPSCALE_IMAGE_TEXT = (
    "⬆️ <b>Повышение качества изображения</b>\n\n"
    "📸 Загрузите изображение, которое хотите улучшить.\n\n"
    "✨ <b>Что делает функция:</b>\n"
    "• Увеличивает разрешение изображения\n"
    "• Улучшает детализацию\n"
    "• Убирает шумы и артефакты\n"
    "• Повышает четкость\n\n"
    "📊 <b>Стоимость:</b>\n"
    "• 1K (1x) - 1 генерация\n"
    "• 2K (2x) - 2 генерации\n"
    "• 4K (4x) - 4 генерации\n"
    "• 8K (8x) - 6 генераций\n\n"
    "💡 Рекомендуем 2K для быстрого улучшения\n\n"
    "⚡ Отправьте изображение для улучшения:"
)

@dp.callback_query(F.data == "check_channel_sub_upscale")
async def process_channel_check_from_upscale(callback: types.CallbackQuery, state: FSMContext):
    """Channel check when coming from upscale — then show upload image"""
    user_id = callback.from_user.id
    is_subscribed = await check_channel_subscription(user_id)
    if is_subscribed:
        already_claimed = db.check_channel_reward_claimed(user_id)
        if already_claimed:
            await callback.answer("✅ Вы уже получали награду!", show_alert=True)
        else:
            success, reason = db.claim_channel_reward(user_id)
            if success:
                await callback.answer("🎉 +1 генерацию!", show_alert=True)
            else:
                await callback.answer("❌ Ошибка начисления", show_alert=True)
    else:
        await callback.answer("❌ Подпишитесь на канал и нажмите снова", show_alert=True)
        return
    await state.set_state(UpscaleImageState.waiting_for_image)
    try:
        await callback.message.edit_text(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())
    except Exception:
        await callback.message.answer(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "skip_channel_sub_upscale")
async def process_skip_channel_from_upscale(callback: types.CallbackQuery, state: FSMContext):
    """Skip channel from upscale flow — show upload image"""
    await callback.answer()
    await state.set_state(UpscaleImageState.waiting_for_image)
    try:
        await callback.message.edit_text(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())
    except Exception:
        await callback.message.answer(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "check_channel_sub_edit")
async def process_channel_check_from_edit(callback: types.CallbackQuery, state: FSMContext):
    """Channel check when coming from edit — then show send images"""
    user_id = callback.from_user.id
    is_subscribed = await check_channel_subscription(user_id)
    if is_subscribed:
        already_claimed = db.check_channel_reward_claimed(user_id)
        if already_claimed:
            await callback.answer("✅ Вы уже получали награду!", show_alert=True)
        else:
            success, reason = db.claim_channel_reward(user_id)
            if success:
                await callback.answer("🎉 +1 генерацию!", show_alert=True)
            else:
                await callback.answer("❌ Ошибка начисления", show_alert=True)
    else:
        await callback.answer("❌ Подпишитесь на канал и нажмите снова", show_alert=True)
        return
    await state.set_state(EditImageState.waiting_for_images)
    await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
    text = "📸 Отправьте до 4 изображений\n\n<i>Можно отправить группой или по одному</i>\n\nЗатем опишите желаемые изменения"
    await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "skip_channel_sub_edit")
async def process_skip_channel_from_edit(callback: types.CallbackQuery, state: FSMContext):
    """Skip channel from edit flow — show send images"""
    await callback.answer()
    await state.set_state(EditImageState.waiting_for_images)
    await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
    text = "📸 Отправьте до 4 изображений\n\n<i>Можно отправить группой или по одному</i>\n\nЗатем опишите желаемые изменения"
    await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle back to main menu button click"""
    await callback.answer()
    await state.clear()

    user_id = callback.from_user.id
    user_name = callback.from_user.first_name or "пользователь"
    
    # Get user info from database
    user_info = db.get_user_info(user_id)
    
    # Format status
    status = user_info["status"]
    if user_info["is_admin"]:
        status_emoji = "👑"
        generations_text = "∞ / ∞"
    else:
        status_emoji = "📊"
        left = user_info["generations_left"]
        limit = user_info["generations_limit"]
        if left == 0:
            status = "—"
            generations_text = "0"
        else:
            generations_text = f"{left} / {limit}"
    
    welcome_text = (
        f"👋 Привет, <b>{user_name}</b>!\n\n"
        f"{status_emoji} <b>Баланс</b>\n"
        f"├ Статус: <b>{status}</b>\n"
        f"└ Генераций: <b>{generations_text}</b>\n\n"
        "🎨 <b>Nano Banana Bot</b>\n"
        "Генерация и редактирование изображений с AI\n\n"
    )
    
    if not user_info["is_admin"] and user_info["generations_left"] == 0:
        welcome_text += "💡 <i>Купите подписку для генераций</i>\n\n"
    
    welcome_text += "Выберите действие:"

    # Try to edit text, if fails (e.g., message with photo), send new message
    try:
        await callback.message.edit_text(welcome_text, reply_markup=get_main_menu_keyboard(user_id))
    except Exception:
        await callback.message.answer(welcome_text, reply_markup=get_main_menu_keyboard(user_id))

@dp.callback_query(F.data == "back_to_prompt")
async def back_to_prompt_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle back to prompt button click"""
    await callback.answer()

    # Get current state
    current_state = await state.get_state()

    if current_state == GenerateImageState.waiting_for_resolution:
        await state.set_state(GenerateImageState.waiting_for_prompt)

        text = "Введите описание изображения, которое вы хотите создать:\n\n<i>Например: 'Уютное кафе с большими окнами, мягкое вечернее освещение'</i>"

        await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())

    elif current_state == EditImageState.waiting_for_resolution:
        await state.set_state(EditImageState.waiting_for_prompt)

        data = await state.get_data()
        images = data.get("images", [])

        text = (
            f"Вы загрузили {len(images)} изображений.\n\n"
            "Теперь введите описание изменений, которые вы хотите применить:\n\n"
            "<i>Например: 'Добавить больше растений и улучшить освещение'</i>"
        )

        await callback.message.edit_text(text, reply_markup=get_back_keyboard("back_to_images"))

@dp.callback_query(F.data == "back_to_images")
async def back_to_images_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle back to images upload button click"""
    await callback.answer()

    await state.set_state(EditImageState.waiting_for_images)

    text = (
        "Отправьте мне до 4 изображений, которые вы хотите отредактировать.\n\n"
        "<i>Вы можете отправить несколько изображений за раз или по одному.</i>"
    )

    await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "show_all_resolutions")
async def show_all_resolutions_callback(callback: types.CallbackQuery, state: FSMContext):
    """Show all available resolutions"""
    await callback.answer()
    user_id = callback.from_user.id
    
    current_state = await state.get_state()
    
    # Determine back callback based on current state
    if current_state in [GenerateImageState.waiting_for_resolution, EditImageState.waiting_for_resolution]:
        back_callback = "back_to_main_resolutions"
    else:
        back_callback = "back_to_menu"
    
    text = (
        "📐 <b>Все форматы изображений:</b>\n\n"
        "Выберите соотношение сторон:"
    )
    
    await callback.message.edit_text(
        text,
        reply_markup=get_resolution_keyboard(back_callback, show_all=True)
    )

@dp.callback_query(F.data == "back_to_main_resolutions")
async def back_to_main_resolutions_callback(callback: types.CallbackQuery, state: FSMContext):
    """Go back to main resolution selection"""
    await callback.answer()
    user_id = callback.from_user.id
    
    text = "📐 Выберите соотношение сторон:"
    
    await callback.message.edit_text(
        text,
        reply_markup=get_resolution_keyboard("back_to_prompt", user_id=user_id)
    )

@dp.callback_query(F.data == "edit_image")
async def edit_image_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle edit image button click"""
    await callback.answer()
    
    user_id = callback.from_user.id
    
    # Check generations first (for all users — paid or free)
    can_generate, msg = db.can_generate(user_id)
    if not can_generate:
        if await should_show_channel_notification(user_id):
            await callback.message.edit_text(
                "⛔ <b>У вас нет доступных генераций</b>\n\n"
                "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                "Там мы делимся лучшими промптами, примерами работ и новостями.",
                reply_markup=get_channel_keyboard(no_generations=True)
            )
        else:
            await callback.message.edit_text(
                f"⛔ <b>У вас нет доступных генераций</b>\n\n{msg}\n\n"
                "💎 Перейдите в магазин, чтобы купить подписку",
                reply_markup=get_main_menu_keyboard(user_id)
            )
        return
    
    # If channel notification needed — show CTA first, then ask for images (same as generate)
    if await should_show_channel_notification(user_id):
        await state.clear()
        await state.set_state(EditImageState.waiting_for_images)
        await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
        await callback.message.edit_text(
            "🎁 <b>Вы можете получить дополнительную 1 генерацию за подписку на канал</b>\n\n"
            "Там мы делимся лучшими промптами, примерами работ и новостями.",
            reply_markup=get_channel_keyboard(from_edit=True)
        )
        return
    
    # Clear any previous state to avoid conflicts
    await state.clear()
    await state.set_state(EditImageState.waiting_for_images)

    text = (
        "📸 Отправьте до 4 изображений\n\n"
        "<i>Можно отправить группой или по одному</i>\n\n"
        "Затем опишите желаемые изменения"
    )

    await callback.message.edit_text(text, reply_markup=get_back_to_menu_keyboard())
    await state.update_data(bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)

@dp.message(EditImageState.waiting_for_images, F.photo)
async def process_edit_images(message: types.Message, state: FSMContext):
    """Process uploaded images for editing"""
    # Check if photo has caption - if yes, use it as prompt and proceed immediately
    has_caption = message.caption and message.caption.strip()
    
    # Check if this is part of a media group
    media_group_id = message.media_group_id
    
    if media_group_id:
        # This is part of a media group, collect all photos
        media_groups[media_group_id].append(message)
        
        # Save caption if this message has one (usually first or last photo in group)
        if has_caption and media_group_id not in media_group_captions:
            media_group_captions[media_group_id] = has_caption
            print(f"DEBUG: Saved caption for media_group {media_group_id}: {has_caption}")
        
        # Wait a bit for other photos in the group
        await asyncio.sleep(0.5)  # Optimized: reduced from 1.0
        
        # Check if we're still the last message in the group
        if message == media_groups[media_group_id][-1]:
            # Process all photos from this media group
            messages_to_process = media_groups[media_group_id].copy()
            
            # Get saved caption for this media group (if any)
            saved_caption = media_group_captions.get(media_group_id)
            
            # Clean up media group storage
            if media_group_id in media_groups:
                del media_groups[media_group_id]
            if media_group_id in media_group_locks:
                del media_group_locks[media_group_id]
            if media_group_id in media_group_captions:
                del media_group_captions[media_group_id]
            
            # Get current images from state
            data = await state.get_data()
            images = data.get("images", [])
            image_urls = data.get("image_urls", [])
            
            # Process all photos from the group
            for msg in messages_to_process:
                if len(images) >= 4:
                    break
                
                photo = msg.photo[-1]
                file = await bot.get_file(photo.file_id)
                telegram_url = get_telegram_file_url(file.file_path)
                image_data = await bot.download_file(file.file_path)
                image_bytes = image_data.read()
                
                images.append(image_bytes)
                image_urls.append(telegram_url)
            
            # Update state and notify user
            await state.update_data(images=images, image_urls=image_urls)
            
            # Use saved caption if available
            print(f"DEBUG: Saved caption for this group: {saved_caption}")
            
            # If caption exists, use it as prompt and proceed to resolution selection
            if saved_caption:
                await state.update_data(prompt=saved_caption)
                await state.set_state(EditImageState.waiting_for_resolution)
                
                user_id = message.from_user.id
                text = (
                    f"✅ Загружено изображений: <b>{len(images)}</b>\n"
                    f"✅ Описание: <b>{saved_caption}</b>\n\n"
                    "📐 Выберите соотношение сторон:"
                )
                
                await message.answer(text, reply_markup=get_resolution_keyboard("back_to_prompt", user_id=user_id))
                return
            
            if len(images) >= 4:
                await state.set_state(EditImageState.waiting_for_prompt)
                text = (
                    f"Вы загрузили {len(images)} изображений.\n\n"
                    "Теперь введите описание изменений, которые вы хотите применить:\n\n"
                    "<i>Например: 'Добавить больше растений и улучшить освещение'</i>"
                )
                await message.answer(text, reply_markup=get_back_keyboard("back_to_images"))
            else:
                text = (
                    f"Загружено {len(images)} изображений! ({len(images)}/4)\n\n"
                    "Отправьте ещё изображения или введите описание изменений, если готовы продолжить."
                )
                await message.answer(text, reply_markup=get_back_to_menu_keyboard())
        # If not the last message, do nothing (wait for the last one)
    else:
        # Single photo, not part of a media group
        data = await state.get_data()
        images = data.get("images", [])
        image_urls = data.get("image_urls", [])

        photo = message.photo[-1]
        file = await bot.get_file(photo.file_id)
        telegram_url = get_telegram_file_url(file.file_path)
        image_data = await bot.download_file(file.file_path)
        image_bytes = image_data.read()

        images.append(image_bytes)
        image_urls.append(telegram_url)
        
        await state.update_data(images=images, image_urls=image_urls)
        
        # If photo has caption, use it as prompt and go to resolution selection
        if has_caption:
            prompt = message.caption.strip()
            await state.update_data(prompt=prompt)
            await state.set_state(EditImageState.waiting_for_resolution)
            
            user_id = message.from_user.id
            text = (
                f"✅ Загружено изображений: <b>{len(images)}</b>\n"
                f"✅ Описание: <b>{prompt}</b>\n\n"
                "📐 Выберите соотношение сторон:"
            )
            
            await message.answer(text, reply_markup=get_resolution_keyboard("back_to_prompt", user_id=user_id))
            return

        if len(images) >= 4:
            await state.set_state(EditImageState.waiting_for_prompt)

            text = (
                f"Вы загрузили {len(images)} изображений.\n\n"
                "Теперь введите описание изменений, которые вы хотите применить:\n\n"
                "<i>Например: 'Добавить больше растений и улучшить освещение'</i>"
            )

            await message.answer(text, reply_markup=get_back_keyboard("back_to_images"))
        else:
            text = (
                f"Изображение загружено! ({len(images)}/4)\n\n"
                "💡 <b>Подсказка:</b> Отправьте фото с подписью чтобы сразу начать редактирование!\n\n"
                "Или отправьте ещё изображения и введите описание изменений."
            )

            await message.answer(text, reply_markup=get_back_to_menu_keyboard())

@dp.message(EditImageState.waiting_for_images, ~F.photo)
async def handle_non_photo_in_edit_mode(message: types.Message, state: FSMContext):
    """Handle non-photo messages when waiting for images"""
    data = await state.get_data()
    images = data.get("images", [])

    if not images:
        await message.answer("Пожалуйста, отправьте изображение для редактирования.", reply_markup=get_back_to_menu_keyboard())
        return

    # If user sends text when we have images, treat it as prompt
    prompt = message.text.strip()
    if prompt:
        await state.set_state(EditImageState.waiting_for_prompt)
        await process_edit_prompt(message, state)
    else:
        await message.answer("Пожалуйста, отправьте изображение или введите описание изменений.", reply_markup=get_back_to_menu_keyboard())

@dp.message(EditImageState.waiting_for_prompt)
async def process_edit_prompt(message: types.Message, state: FSMContext):
    """Process the prompt for image editing"""
    prompt = message.text.strip()
    user_id = message.from_user.id

    if not prompt:
        await message.answer("Пожалуйста, введите непустое описание изменений.", reply_markup=get_back_keyboard("back_to_images"))
        return

    # Store the prompt
    await state.update_data(prompt=prompt)
    await state.set_state(EditImageState.waiting_for_resolution)

    text = (
        f"Вы ввели описание изменений: <b>{prompt}</b>\n\n"
        "Теперь выберите соотношение сторон:"
    )

    await message.answer(text, reply_markup=get_resolution_keyboard("back_to_prompt", user_id=user_id))

@dp.callback_query(EditImageState.waiting_for_resolution, F.data.startswith("resolution_"))
async def process_edit_resolution(callback: types.CallbackQuery, state: FSMContext):
    """Process resolution selection for image editing"""
    user_id = callback.from_user.id
    
    # Check if user can generate
    can_generate, msg = db.can_generate(user_id)
    if not can_generate:
        await callback.answer("⛔ Нет генераций", show_alert=True)
        await state.clear()
        if await should_show_channel_notification(user_id):
            await callback.message.edit_text(
                "⛔ <b>У вас нет доступных генераций</b>\n\n"
                "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                "Там мы делимся лучшими промптами, примерами работ и новостями.",
                reply_markup=get_channel_keyboard(no_generations=True)
            )
        else:
            await callback.message.edit_text(
                f"⛔ <b>У вас нет доступных генераций</b>\n\n{msg}\n\n"
                "💎 Перейдите в магазин, чтобы купить подписку",
                reply_markup=get_main_menu_keyboard(user_id)
            )
        return
    
    resolution_key = callback.data.replace("resolution_", "")
    resolution = RESOLUTIONS.get(resolution_key, "16:9")
    
    # Auto-save user's preferred resolution
    db.set_user_preferred_resolution(user_id, resolution)

    # Get stored data
    data = await state.get_data()
    images = data.get("images", [])
    image_urls = data.get("image_urls", [])
    prompt = data.get("prompt")

    # Clear state
    await state.clear()

    # Answer callback immediately and update message simultaneously
    await callback.answer("⏳ Редактирование началось...")
    progress_message = await callback.message.edit_text(
        "✏️ <b>Редактирование началось...</b>\n\n"
        "⏳ Обрабатываю изображения, подождите немного..."
    )

    # Record generation usage
    db.use_generation(user_id, prompt, "edit")
    
    # Check and claim referral reward for referrer (if this is referred user's first generation)
    try:
        referrer_id = db.get_referrer_id(user_id)
        if referrer_id:
            # Claim reward (will only work once per referral)
            if db.claim_referral_reward(referrer_id, user_id, reward_generations=2):
                # Notify referrer
                try:
                    await bot.send_message(
                        referrer_id,
                        "🎉 <b>Реферальная награда!</b>\n\n"
                        f"Ваш друг сделал первую генерацию!\n"
                        f"Вы получили <b>1 генерацию</b> 🎁"
                    )
                except Exception:
                    pass  # Referrer might have blocked the bot
    except Exception as e:
        logger.error(f"Error processing referral reward: {e}")

    # Edit image with progress updates
    import time
    start_gen = time.time()
    
    # Create background task for progress updates
    async def update_progress():
        dots = 0
        while True:
            await asyncio.sleep(3)
            dots = (dots + 1) % 4
            elapsed = int(time.time() - start_gen)
            try:
                prompt_text = f"{prompt[:50]}..." if len(prompt) > 50 else prompt
                await progress_message.edit_text(
                    f"✏️ <b>Редактирование изображения</b>{'.' * (dots + 1)}\n\n"
                    f"⏳ Прошло: {elapsed} сек\n"
                    f"💭 Изменения: <i>{prompt_text}</i>"
                )
            except Exception as e:
                logger.debug(f"Progress update error: {e}")
                pass
    
    progress_task = asyncio.create_task(update_progress())
    
    error_message = None
    image_data = None
    try:
        image_data = await asyncio.wait_for(
            edit_image_via_api(images, prompt, resolution, None),
            timeout=180.0
        )
    except asyncio.TimeoutError:
        logger.error("❌ Edit timeout")
        error_message = "TIMEOUT"
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Edit error: {error_msg}")
        if "API_ERROR:" in error_msg:
            error_message = error_msg.replace("API_ERROR: ", "")
        elif "TIMEOUT_ERROR:" in error_msg:
            error_message = "TIMEOUT"
        else:
            error_message = "UNKNOWN"
    finally:
        progress_task.cancel()
        try:
            await progress_task
        except asyncio.CancelledError:
            pass

    if image_data:
        # Send the edited image
        sent_message = await callback.message.answer_photo(
            photo=types.BufferedInputFile(image_data, filename="edited_image.jpg"),
            caption="Вот ваше отредактированное изображение:",
            reply_markup=get_generated_image_keyboard()
        )
        # Save file_id for potential upscale (both in state and database)
        file_id = sent_message.photo[-1].file_id
        await state.update_data(last_generated_image_file_id=file_id)
        db.save_last_generated_image(user_id, file_id)
        # Channel CTA after edit (same rules: free=every time, paid=every 4)
        user_info = db.get_user_info(user_id)
        remaining = user_info.get("generations_left", 0)
        status_text = f"💎 Осталось генераций: {remaining}"
        if await should_show_channel_after_generation(user_id):
            await callback.message.answer(
                status_text + "\n\n🎁 Подпишись на канал и получи ещё +1 генерацию!",
                reply_markup=get_channel_keyboard()
            )
        else:
            await callback.message.answer(status_text)
    else:
        # Формируем сообщение об ошибке в зависимости от типа
        if error_message and error_message != "TIMEOUT" and error_message != "UNKNOWN":
            # Конкретная ошибка от API
            error_text = (
                f"❌ <b>Ошибка при редактировании изображения</b>\n\n"
                f"<b>Причина:</b> {error_message}\n\n"
                f"💡 Попробуйте:\n"
                f"• Изменить описание изменений\n"
                f"• Упростить промпт\n"
                f"• Попробовать позже"
            )
        elif error_message == "TIMEOUT":
            error_text = (
                "⏱️ <b>Превышено время ожидания</b>\n\n"
                "Редактирование заняло слишком много времени (>60 сек).\n\n"
                "💡 Попробуйте:\n"
                "• Упростить описание изменений\n"
                "• Попробовать еще раз\n"
                "• Проверить подключение к интернету"
            )
        else:
            # Общая ошибка
            error_text = (
                "❌ Произошла ошибка при редактировании изображения.\n\n"
                "<b>Возможные причины:</b>\n"
                "• Ваш запрос нарушает политику использования AI\n"
                "• Проблемы с API сервером\n"
                "• Превышен лимит запросов\n\n"
                "💡 Попробуйте:\n"
                "• Изменить описание изменений\n"
                "• Упростить промпт\n"
                "• Попробовать позже"
            )
        
        await callback.message.answer(
            error_text,
            reply_markup=get_main_menu_keyboard(user_id)
        )

# ============================================
# SHOP / PAYMENT HANDLERS
# ============================================

@dp.callback_query(F.data == "shop")
async def show_shop(callback: types.CallbackQuery):
    """Show shop with subscription plans"""
    await callback.answer()
    
    shop_text = (
        "💎 <b>Nano Banana Bot</b> — магазин подписок\n\n"
        "Генерируйте и редактируйте изображения с помощью AI!\n\n"
        "<b>Выберите тариф:</b>\n\n"
        
        "🌟 <b>МИНИ</b> — 149₽\n"
        "   • 5 генераций / 30 дней\n\n"
        
        "🟢 <b>STARTER</b> — 249₽\n"
        "   • 10 генераций\n\n"
        
        "🔵 <b>PRO</b> — 599₽\n"
        "   • 30 генераций\n\n"
        
        "⭐ <b>UNLIMITED</b> — 1490₽\n"
        "   • 90 генераций\n\n"
        
        "Выберите тариф:"
    )
    
    # Create shop keyboard
    builder = InlineKeyboardBuilder()
    builder.button(text="🌟 МИНИ — 149₽", callback_data="buy_mini")
    builder.button(text="🟢 STARTER — 249₽", callback_data="buy_starter")
    builder.button(text="🔵 PRO — 599₽", callback_data="buy_pro")
    builder.button(text="⭐ UNLIMITED — 1490₽", callback_data="buy_unlimited")
    builder.button(text="🆘 Помощь", callback_data="help_support")
    builder.button(text="🏠 Главное меню", callback_data="back_to_menu")
    builder.adjust(1)
    
    try:
        await callback.message.edit_text(shop_text, reply_markup=builder.as_markup())
    except Exception:
        await callback.message.answer(shop_text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("buy_"))
async def process_purchase(callback: types.CallbackQuery):
    """Process subscription purchase"""
    await callback.answer()
    
    plan = callback.data.replace("buy_", "")
    
    plans = {
        "mini": {
            "name": "🌟 МИНИ",
            "price": 100,
            "generations": 10,
            "duration_days": 30,
            "emoji": "🌟"
        },
        "starter": {
            "name": "STARTER",
            "price": 249,
            "generations": 10,
            "emoji": "🟢"
        },
        "pro": {
            "name": "PRO",
            "price": 399,
            "generations": 30,
            "emoji": "🔵"
        },
        "unlimited": {
            "name": "UNLIMITED",
            "price": 899,
            "generations": 90,
            "emoji": "⭐"
        }
    }
    
    plan_info = plans.get(plan)
    if not plan_info:
        await callback.message.answer("Ошибка: тариф не найден")
        return
    
    payment_text = (
        f"{plan_info['emoji']} <b>Тариф {plan_info['name']}</b>\n\n"
        f"💰 Стоимость: {plan_info['price']}₽/месяц\n"
        f"🎨 Генераций: {plan_info['generations']} в месяц\n\n"
        f"Выберите способ оплаты:\n\n"
        f"💡 <i>Оплата картой доступна в приложении Telegram (телефон или ПК). "
        f"В браузере счёт может не отображаться — откройте приложение.</i>"
    )
    
    # Create payment method keyboard
    builder = InlineKeyboardBuilder()
    builder.button(text="💳 Оплатить картой (YooKassa)", callback_data=f"pay_yookassa_{plan}")
    builder.button(text="⭐ Оплатить Telegram Stars", callback_data=f"pay_stars_{plan}")
    builder.button(text="⬅️ Назад к тарифам", callback_data="shop")
    builder.adjust(1)
    
    try:
        await callback.message.edit_text(payment_text, reply_markup=builder.as_markup())
    except Exception:
        await callback.message.answer(payment_text, reply_markup=builder.as_markup())

@dp.callback_query(F.data.startswith("pay_yookassa_"))
async def pay_yookassa(callback: types.CallbackQuery):
    """Create payment invoice via YooKassa"""
    await callback.answer()
    
    if not PAYMENT_TOKEN:
        await callback.message.answer(
            "❌ Система оплаты временно недоступна.\n"
            "Свяжитесь с поддержкой.",
            reply_markup=get_back_to_menu_keyboard()
        )
        return
    
    plan = callback.data.replace("pay_yookassa_", "")
    
    plans = {
        "mini": {"name": "МИНИ", "price": 149, "generations": 5, "emoji": "🌟"},
        "starter": {"name": "STARTER", "price": 249, "generations": 10, "emoji": "🟢"},
        "pro": {"name": "PRO", "price": 599, "generations": 30, "emoji": "🔵"},
        "unlimited": {"name": "UNLIMITED", "price": 1490, "generations": 90, "emoji": "⭐"}
    }
    
    plan_info = plans.get(plan)
    if not plan_info:
        await callback.message.answer("❌ Тариф не найден")
        return
    
    user_id = callback.from_user.id
    
    # Create payload for identification
    payload = f"{plan}_{user_id}_{int(datetime.now().timestamp())}"
    
    try:
        # Send invoice
        await bot.send_invoice(
            chat_id=user_id,
            title=f"{plan_info['emoji']} Тариф {plan_info['name']}",
            description=f"🎨 {plan_info['generations']} генераций изображений с помощью AI",
            payload=payload,
            provider_token=PAYMENT_TOKEN,
            currency="RUB",
            prices=[
                types.LabeledPrice(
                    label=f"Тариф {plan_info['name']}", 
                    amount=int(plan_info['price'] * 100)  # В копейках, целое число
                )
            ],
            max_tip_amount=0,
            suggested_tip_amounts=[],
            need_name=False,
            need_phone_number=False,
            need_email=False,
            need_shipping_address=False,
            is_flexible=False
        )
        
        mode = "тестовом" if USE_TEST_PAYMENTS else "боевом"
        logger.info(f"📝 Invoice sent to user {user_id} for {plan} in {mode} mode")
        
    except Exception as e:
        import traceback
        err_text = str(e)
        logger.error(f"Error sending invoice for {plan}: {err_text}")
        logger.error(traceback.format_exc())
        # Если Telegram отклоняет токен — подсказка
        if "PAYMENT_PROVIDER_INVALID" in err_text:
            msg = (
                "❌ <b>Ошибка провайдера платежей</b>\n\n"
                "Telegram не принимает токен ЮKassa.\n\n"
                "Проверьте в @BotFather:\n"
                "• /mybots → ваш бот → Payments → ЮKassa\n"
                "• Должно быть: «Connect YooMoney» и авторизация в боте ЮKassa.\n\n"
                "Или попробуйте отключить и заново подключить ЮKassa в Payments."
            )
        else:
            msg = (
                "❌ Ошибка создания счета.\n"
                "Попробуйте позже или свяжитесь с поддержкой."
            )
        await callback.message.answer(msg, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data.startswith("pay_stars_"))
async def pay_stars(callback: types.CallbackQuery):
    """Process Telegram Stars payment"""
    await callback.answer("🔧 Скоро!", show_alert=True)
    
    await callback.message.edit_text(
        "🔧 <b>Оплата Telegram Stars - в разработке</b>\n\n"
        "Мы работаем над интеграцией Telegram Stars для удобной оплаты.\n\n"
        "💫 <b>Скоро будет доступно!</b>\n\n"
        "Пока вы можете связаться с поддержкой для ручного оформления подписки.",
        reply_markup=get_back_to_menu_keyboard()
    )

@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: types.PreCheckoutQuery):
    """
    Handle pre-checkout query from Telegram.
    Telegram asks: "Can we charge the user?"
    We must answer within 10 seconds!
    """
    user_id = pre_checkout_query.from_user.id
    
    # Можно добавить дополнительные проверки здесь
    # Например, проверить что пользователь не забанен и т.д.
    
    logger.info(f"💳 Pre-checkout query from user {user_id}")
    
    # Подтверждаем платеж
    await bot.answer_pre_checkout_query(
        pre_checkout_query.id,
        ok=True
    )
    
    # Если нужно отклонить платеж:
    # await bot.answer_pre_checkout_query(
    #     pre_checkout_query.id,
    #     ok=False,
    #     error_message="Причина отклонения"
    # )

@dp.message(F.successful_payment)
async def process_successful_payment(message: types.Message):
    """Handle successful payment"""
    payment = message.successful_payment
    user_id = message.from_user.id
    
    # Parse payload
    payload_parts = payment.invoice_payload.split("_")
    plan = payload_parts[0] if len(payload_parts) > 0 else "unknown"
    
    plans = {
        "mini": {"name": "МИНИ", "price": 149, "generations": 5, "emoji": "🌟"},
        "starter": {"name": "STARTER", "price": 249, "generations": 10, "emoji": "🟢"},
        "pro": {"name": "PRO", "price": 599, "generations": 30, "emoji": "🔵"},
        "unlimited": {"name": "UNLIMITED", "price": 1490, "generations": 90, "emoji": "⭐"}
    }
    
    plan_info = plans.get(plan, {"name": "UNKNOWN", "generations": 0, "emoji": "❓"})
    
    # ЗАЩИТА: Проверяем дубликат платежа ПЕРЕД начислением
    payment_id = payment.telegram_payment_charge_id
    if db.payment_exists(payment_id):
        logger.warning(f"⚠️ Duplicate payment detected: {payment_id}")
        await message.answer("✅ Этот платёж уже обработан!")
        return
    
    # Логируем платёж СРАЗУ (до начисления генераций)
    db.log_payment(
        user_id=user_id,
        telegram_charge_id=payment.telegram_payment_charge_id,
        provider_charge_id=payment.provider_payment_charge_id,
        plan_type=plan,
        amount=payment.total_amount,
        generations_added=plan_info['generations']
    )
    
    # ТЕПЕРЬ начисляем генерации (платёж уже в базе)
    success = db.add_generations(user_id, plan_info['generations'])
    if not success:
        # No active subscription - create one
        db.add_subscription(user_id, plan_info['name'], plan_info['generations'], 30)
    
    # Get user info
    user = message.from_user
    username = f"@{user.username}" if user.username else user.first_name
    
    # Notify admin
    for admin_id in ADMIN_IDS:
        try:
            admin_text = (
                "💰 <b>НОВАЯ ОПЛАТА!</b>\n\n"
                f"👤 Пользователь: {username}\n"
                f"🆔 ID: <code>{user_id}</code>\n"
                f"{plan_info['emoji']} Тариф: <b>{plan_info['name']}</b>\n"
                f"💵 Сумма: <b>{payment.total_amount / 100:.2f} ₽</b>\n"
                f"🎨 Генераций: <b>{plan_info['generations']}</b>\n\n"
                f"🔑 Telegram Payment ID:\n<code>{payment.telegram_payment_charge_id}</code>\n"
                f"🏦 Provider Payment ID:\n<code>{payment.provider_payment_charge_id}</code>"
            )
            await bot.send_message(admin_id, admin_text)
        except Exception as e:
            logger.error(f"Error notifying admin {admin_id}: {e}")
    
    # Thank user
    await message.answer(
        f"✅ <b>Оплата успешна!</b>\n\n"
        f"{plan_info['emoji']} Тариф: <b>{plan_info['name']}</b>\n"
        f"💰 Оплачено: <b>{payment.total_amount / 100:.2f} ₽</b>\n"
        f"🎨 Добавлено генераций: <b>+{plan_info['generations']}</b>\n\n"
        f"💫 Спасибо за покупку!\n"
        f"Теперь вы можете создавать изображения ✨",
        reply_markup=get_main_menu_keyboard(user_id)
    )
    
    logger.info(f"✅ Payment completed: {plan} for user {user_id}, amount: {payment.total_amount/100:.2f} RUB")

# ============================================
# ADMIN PANEL HANDLERS
# ============================================

def is_admin(user_id: int) -> bool:
    """Check if user is admin"""
    return user_id in ADMIN_IDS

@dp.message(Command("test_api"))
async def test_api_command(message: types.Message):
    """Test API response time"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer("⛔ У вас нет доступа к этой команде")
        return
    
    status_msg = await message.answer("🧪 <b>Тестирую API...</b>")
    
    import time
    test_prompt = "a small yellow banana"
    
    # Test generation
    start = time.time()
    try:
        result = await generate_image_via_api(test_prompt, "1:1")
        elapsed = time.time() - start
        
        if result:
            await status_msg.edit_text(
                f"✅ <b>API работает!</b>\n\n"
                f"⏱ Время генерации: <b>{elapsed:.2f} сек</b>\n"
                f"📦 Размер изображения: <b>{len(result) / 1024:.2f} KB</b>\n"
                f"🎨 Тест: <code>{test_prompt}</code>"
            )
        else:
            await status_msg.edit_text(
                f"❌ <b>API вернул ошибку</b>\n\n"
                f"⏱ Время попытки: <b>{elapsed:.2f} сек</b>\n"
                f"📝 Проверьте логи для деталей"
            )
    except Exception as e:
        elapsed = time.time() - start
        await status_msg.edit_text(
            f"❌ <b>Ошибка при тестировании</b>\n\n"
            f"⏱ Время: <b>{elapsed:.2f} сек</b>\n"
            f"❗ Ошибка: <code>{str(e)}</code>"
        )

@dp.message(Command("admin"))
async def admin_panel_command(message: types.Message, state: FSMContext):
    """Handle /admin command"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer("⛔ У вас нет доступа к админ-панели")
        return
    
    await state.clear()
    stats = db.get_stats()
    payment_stats = db.get_payment_stats()
    
    admin_text = (
        "👑 <b>АДМИН-ПАНЕЛЬ</b>\n\n"
        "📊 <b>Статистика пользователей:</b>\n"
        f"├ Всего пользователей: <b>{stats['total_users']}</b>\n"
        f"├ Активных подписок: <b>{stats['active_subscriptions']}</b>\n"
        f"├ Всего генераций: <b>{stats['total_generations']}</b>\n"
        f"├ Новых сегодня: <b>{stats['today_users']}</b>\n"
        f"└ Генераций сегодня: <b>{stats['today_generations']}</b>\n\n"
        "💰 <b>Статистика платежей:</b>\n"
        f"├ Всего платежей: <b>{payment_stats['total_payments']}</b>\n"
        f"├ Общая выручка: <b>{payment_stats['total_revenue']:.2f} ₽</b>\n"
        f"├ Платежей сегодня: <b>{payment_stats['today_payments']}</b>\n"
        f"└ Выручка сегодня: <b>{payment_stats['today_revenue']:.2f} ₽</b>\n\n"
        "💡 <i>Используйте /test_api для проверки API</i>\n\n"
        "Выберите действие:"
    )
    
    await message.answer(admin_text, reply_markup=get_admin_menu_keyboard())

@dp.callback_query(F.data == "admin_panel")
async def admin_panel_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle admin panel button"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    await state.clear()
    
    stats = db.get_stats()
    payment_stats = db.get_payment_stats()
    
    admin_text = (
        "👑 <b>АДМИН-ПАНЕЛЬ</b>\n\n"
        "📊 <b>Статистика пользователей:</b>\n"
        f"├ Всего пользователей: <b>{stats['total_users']}</b>\n"
        f"├ Активных подписок: <b>{stats['active_subscriptions']}</b>\n"
        f"├ Всего генераций: <b>{stats['total_generations']}</b>\n"
        f"├ Новых сегодня: <b>{stats['today_users']}</b>\n"
        f"└ Генераций сегодня: <b>{stats['today_generations']}</b>\n\n"
        "💰 <b>Статистика платежей:</b>\n"
        f"├ Всего платежей: <b>{payment_stats['total_payments']}</b>\n"
        f"├ Общая выручка: <b>{payment_stats['total_revenue']:.2f} ₽</b>\n"
        f"├ Платежей сегодня: <b>{payment_stats['today_payments']}</b>\n"
        f"└ Выручка сегодня: <b>{payment_stats['today_revenue']:.2f} ₽</b>\n\n"
        "Выберите действие:"
    )
    
    await callback.message.edit_text(admin_text, reply_markup=get_admin_menu_keyboard())

@dp.callback_query(F.data == "admin_stats")
async def admin_stats_callback(callback: types.CallbackQuery):
    """Show detailed statistics"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    stats = db.get_stats()
    
    stats_text = (
        "📊 <b>ДЕТАЛЬНАЯ СТАТИСТИКА</b>\n\n"
        f"👥 Всего пользователей: <b>{stats['total_users']}</b>\n"
        f"   └ Новых сегодня: <b>{stats['today_users']}</b>\n\n"
        f"💎 Активных подписок: <b>{stats['active_subscriptions']}</b>\n\n"
        f"🎨 Всего генераций: <b>{stats['total_generations']}</b>\n"
        f"   └ Сегодня: <b>{stats['today_generations']}</b>\n"
    )
    
    await callback.message.edit_text(stats_text, reply_markup=get_admin_back_keyboard())

@dp.callback_query(F.data == "admin_search")
async def admin_search_callback(callback: types.CallbackQuery, state: FSMContext):
    """Start user search"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    await state.set_state(AdminState.search_user)
    
    search_text = (
        "🔍 <b>ПОИСК ПОЛЬЗОВАТЕЛЯ</b>\n\n"
        "Введите <b>User ID</b> или <b>Username</b> пользователя:\n\n"
        "<i>Например: 123456789 или username</i>"
    )
    
    await callback.message.edit_text(search_text, reply_markup=get_admin_back_keyboard())

@dp.message(AdminState.search_user)
async def process_user_search(message: types.Message, state: FSMContext):
    """Process user search query"""
    query = message.text.strip()
    
    # Delete user message
    try:
        await message.delete()
    except:
        pass
    
    results = db.search_user(query)
    
    if not results:
        await message.answer(
            f"❌ Пользователь не найден: <code>{query}</code>",
            reply_markup=get_admin_back_keyboard()
        )
        return
    
    # Show results
    for user in results:
        user_id, username, first_name, last_name, created_at, last_active = user
        
        # Get full info
        full_info = db.get_user_full_info(user_id)
        user_info = db.get_user_info(user_id)
        
        name = first_name or "Нет имени"
        username_text = f"@{username}" if username else "Нет username"
        
        info_text = (
            f"👤 <b>ПОЛЬЗОВАТЕЛЬ</b>\n\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"👤 Имя: {name}\n"
            f"📱 Username: {username_text}\n"
            f"📅 Регистрация: {created_at[:10]}\n"
            f"🕐 Последняя активность: {last_active[:16]}\n\n"
            f"💎 <b>Статус:</b> {user_info['status']}\n"
            f"🎨 <b>Генераций:</b> {user_info['generations_used']} / {user_info['generations_limit']}\n"
            f"📊 <b>Осталось:</b> {user_info['generations_left']}\n"
        )
        
        if full_info['is_admin']:
            info_text += "\n👑 <b>АДМИНИСТРАТОР</b>"
        
        await message.answer(info_text, reply_markup=get_user_manage_keyboard(user_id))
    
    await state.clear()

@dp.callback_query(F.data.startswith("admin_user_"))
async def admin_show_user(callback: types.CallbackQuery):
    """Show user info"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    target_user_id = int(callback.data.split("_")[2])
    full_info = db.get_user_full_info(target_user_id)
    
    if not full_info:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    user_info = db.get_user_info(target_user_id)
    
    name = full_info['first_name'] or "Нет имени"
    username_text = f"@{full_info['username']}" if full_info['username'] else "Нет username"
    
    info_text = (
        f"👤 <b>ПОЛЬЗОВАТЕЛЬ</b>\n\n"
        f"🆔 ID: <code>{target_user_id}</code>\n"
        f"👤 Имя: {name}\n"
        f"📱 Username: {username_text}\n"
        f"📅 Регистрация: {full_info['created_at'][:10]}\n"
        f"🕐 Последняя активность: {full_info['last_active'][:16]}\n\n"
        f"💎 <b>Статус:</b> {user_info['status']}\n"
        f"🎨 <b>Генераций:</b> {user_info['generations_used']} / {user_info['generations_limit']}\n"
        f"📊 <b>Осталось:</b> {user_info['generations_left']}\n"
    )
    
    if full_info['is_admin']:
        info_text += "\n👑 <b>АДМИНИСТРАТОР</b>"
    
    await callback.message.edit_text(info_text, reply_markup=get_user_manage_keyboard(target_user_id))

@dp.callback_query(F.data.startswith("admin_give_sub_"))
async def admin_give_subscription(callback: types.CallbackQuery):
    """Show subscription plans to give"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    target_user_id = int(callback.data.split("_")[3])
    
    text = (
        f"➕ <b>ВЫДАТЬ ПОДПИСКУ</b>\n\n"
        f"User ID: <code>{target_user_id}</code>\n\n"
        "Выберите тарифный план:"
    )
    
    await callback.message.edit_text(text, reply_markup=get_subscription_plans_keyboard(target_user_id))

@dp.callback_query(F.data.startswith("admin_sub_"))
async def admin_process_subscription(callback: types.CallbackQuery):
    """Process subscription giving"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    parts = callback.data.split("_")
    plan = parts[2]
    target_user_id = int(parts[3])
    
    plans = {
        "mini": {"name": "MINI", "generations": 5},
        "starter": {"name": "STARTER", "generations": 10},
        "pro": {"name": "PRO", "generations": 30},
        "unlimited": {"name": "UNLIMITED", "generations": 90}
    }
    
    plan_info = plans.get(plan)
    if not plan_info:
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    # Give subscription
    db.add_subscription(target_user_id, plan_info["name"], plan_info["generations"], 30)
    
    await callback.answer(f"✅ Подписка {plan_info['name']} выдана!", show_alert=True)
    
    # Show updated user info
    full_info = db.get_user_full_info(target_user_id)
    user_info = db.get_user_info(target_user_id)
    
    name = full_info['first_name'] or "Нет имени"
    
    info_text = (
        f"✅ <b>ПОДПИСКА ВЫДАНА</b>\n\n"
        f"👤 Пользователь: {name}\n"
        f"🆔 ID: <code>{target_user_id}</code>\n\n"
        f"💎 Тариф: <b>{plan_info['name']}</b>\n"
        f"🎨 Генераций: <b>{plan_info['generations']}</b>\n"
        f"📅 Срок: <b>30 дней</b>\n"
    )
    
    await callback.message.edit_text(info_text, reply_markup=get_user_manage_keyboard(target_user_id))

@dp.callback_query(F.data.startswith("admin_cancel_sub_"))
async def admin_cancel_subscription(callback: types.CallbackQuery):
    """Cancel user subscription"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    target_user_id = int(callback.data.split("_")[3])
    
    success = db.cancel_subscription(target_user_id)
    
    if success:
        await callback.answer("✅ Подписка отменена", show_alert=True)
    else:
        await callback.answer("❌ Нет активной подписки", show_alert=True)
    
    # Show updated user info
    full_info = db.get_user_full_info(target_user_id)
    user_info = db.get_user_info(target_user_id)
    
    name = full_info['first_name'] or "Нет имени"
    username_text = f"@{full_info['username']}" if full_info['username'] else "Нет username"
    
    info_text = (
        f"👤 <b>ПОЛЬЗОВАТЕЛЬ</b>\n\n"
        f"🆔 ID: <code>{target_user_id}</code>\n"
        f"👤 Имя: {name}\n"
        f"📱 Username: {username_text}\n\n"
        f"💎 <b>Статус:</b> {user_info['status']}\n"
        f"🎨 <b>Генераций:</b> {user_info['generations_used']} / {user_info['generations_limit']}\n"
        f"📊 <b>Осталось:</b> {user_info['generations_left']}\n"
    )
    
    await callback.message.edit_text(info_text, reply_markup=get_user_manage_keyboard(target_user_id))

@dp.callback_query(F.data.startswith("admin_add_gen_"))
async def admin_add_generations_start(callback: types.CallbackQuery, state: FSMContext):
    """Start adding generations"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    target_user_id = int(callback.data.split("_")[3])
    await state.set_state(AdminState.add_generations)
    await state.update_data(target_user_id=target_user_id)
    
    text = (
        f"🎁 <b>ДОБАВИТЬ ГЕНЕРАЦИИ</b>\n\n"
        f"User ID: <code>{target_user_id}</code>\n\n"
        "Введите количество генераций для добавления:\n\n"
        "<i>Например: 10</i>"
    )
    
    await callback.message.edit_text(text, reply_markup=get_admin_back_keyboard())

@dp.message(AdminState.add_generations)
async def admin_add_generations_process(message: types.Message, state: FSMContext):
    """Process adding generations"""
    try:
        amount = int(message.text.strip())
        
        if amount <= 0:
            raise ValueError
        
        data = await state.get_data()
        target_user_id = data.get("target_user_id")
        
        # Delete user message
        try:
            await message.delete()
        except:
            pass
        
        success = db.add_generations(target_user_id, amount)
        
        if success:
            await message.answer(
                f"✅ Добавлено <b>{amount}</b> генераций пользователю <code>{target_user_id}</code>",
                reply_markup=get_admin_back_keyboard()
            )
        else:
            await message.answer(
                f"❌ У пользователя <code>{target_user_id}</code> нет активной подписки",
                reply_markup=get_admin_back_keyboard()
            )
        
        await state.clear()
        
    except ValueError:
        await message.answer(
            "❌ Введите корректное число",
            reply_markup=get_admin_back_keyboard()
        )

@dp.callback_query(F.data == "admin_users")
async def admin_users_list(callback: types.CallbackQuery):
    """Show recent users"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    users = db.get_all_users(limit=10)
    
    text = "👥 <b>ПОСЛЕДНИЕ ПОЛЬЗОВАТЕЛИ</b>\n\n"
    
    for user in users:
        user_id_item, username, first_name, created_at, last_active = user
        name = first_name or "Нет имени"
        username_text = f"@{username}" if username else "—"
        
        text += (
            f"👤 {name} ({username_text})\n"
            f"   ID: <code>{user_id_item}</code>\n"
            f"   Активность: {last_active[:16]}\n\n"
        )
    
    await callback.message.edit_text(text, reply_markup=get_admin_back_keyboard())

@dp.callback_query(F.data == "admin_generations")
async def admin_generations_list(callback: types.CallbackQuery):
    """Show recent generations"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    
    generations = db.get_recent_generations(limit=10)
    
    text = "🎨 <b>ПОСЛЕДНИЕ ГЕНЕРАЦИИ</b>\n\n"
    
    for gen in generations:
        gen_user_id, username, first_name, prompt, gen_type, created_at = gen
        name = first_name or "Нет имени"
        username_text = f"@{username}" if username else "—"
        
        # Truncate prompt
        short_prompt = prompt[:50] + "..." if len(prompt) > 50 else prompt
        
        emoji = "🎨" if gen_type == "generate" else "✏️"
        
        text += (
            f"{emoji} {name} ({username_text})\n"
            f"   <code>{short_prompt}</code>\n"
            f"   {created_at[:16]}\n\n"
        )
    
    await callback.message.edit_text(text, reply_markup=get_admin_back_keyboard())

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    """Start broadcast"""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await callback.answer()
    await state.set_state(AdminState.broadcast_message)
    
    text = (
        "📢 <b>BROADCAST</b>\n\n"
        "Введите сообщение для отправки всем пользователям:\n\n"
        "<i>Можно использовать HTML разметку</i>"
    )
    
    await callback.message.edit_text(text, reply_markup=get_admin_back_keyboard())

@dp.message(AdminState.broadcast_message)
async def admin_broadcast_process(message: types.Message, state: FSMContext):
    """Process broadcast message"""
    broadcast_text = message.text
    
    # Delete user message
    try:
        await message.delete()
    except:
        pass
    
    status_msg = await message.answer("📢 Начинаю рассылку...")
    
    users = db.get_all_users(limit=10000)
    
    sent = 0
    failed = 0
    
    for user in users:
        user_id_item = user[0]
        try:
            await bot.send_message(user_id_item, broadcast_text)
            sent += 1
            await asyncio.sleep(0.05)  # Rate limiting
        except Exception as e:
            failed += 1
            logger.error(f"Failed to send broadcast to {user_id_item}: {e}")
    
    result_text = (
        f"✅ <b>РАССЫЛКА ЗАВЕРШЕНА</b>\n\n"
        f"✅ Отправлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{failed}</b>\n"
        f"📊 Всего: <b>{sent + failed}</b>"
    )
    
    await status_msg.edit_text(result_text, reply_markup=get_admin_back_keyboard())
    await state.clear()

# ===== PROMOCODE HANDLERS =====

@dp.callback_query(F.data == "enter_promocode")
async def enter_promocode_handler(callback: types.CallbackQuery, state: FSMContext):
    """Handle enter promocode button"""
    await callback.answer()
    await state.set_state(PromocodeState.waiting_for_code)
    
    await callback.message.edit_text(
        "🎁 <b>Активация промокода</b>\n\n"
        "Введите промокод для получения бонусов:\n\n"
        "💡 <i>Промокоды дают дополнительные генерации или подписки</i>",
        reply_markup=get_back_to_menu_keyboard()
    )
    
    # Save message info for later editing
    await state.update_data(
        bot_message_id=callback.message.message_id,
        chat_id=callback.message.chat.id
    )

@dp.message(PromocodeState.waiting_for_code, F.text)
async def process_promocode(message: types.Message, state: FSMContext):
    """Process promocode input (case-insensitive)"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    code = (message.text or "").strip()
    
    if not code:
        await message.answer("❌ Введите текст промокода", reply_markup=get_back_to_menu_keyboard())
        return
    
    try:
        await message.delete()
    except Exception:
        pass
    
    data = await state.get_data()
    bot_message_id = data.get("bot_message_id")
    state_chat_id = data.get("chat_id")
    
    success, msg, reward = db.use_promocode(user_id, code)
    
    if success:
        reward_text = ""
        if reward["reward_type"] == "generations":
            reward_text = f"🎉 Вы получили <b>{reward['reward_value']}</b> генераций!"
        elif reward["reward_type"] == "subscription":
            plan_name = reward.get("plan_name", "?")
            gens = reward.get("gens", 0)
            if gens == 1:
                gens_text = "1 генерация"
            elif 2 <= gens <= 4:
                gens_text = f"{gens} генерации"
            else:
                gens_text = f"{gens} генераций"
            applied = reward.get("promo_applied", "new")
            if applied == "added":
                reward_text = f"🎉 Вам добавлено {gens_text} к текущей подписке!"
            else:
                reward_text = f"🎉 Вы получили подписку <b>{plan_name}</b> ({gens_text})!"
        
        result_text = (
            f"✅ <b>Промокод активирован!</b>\n\n"
            f"{reward_text}\n\n"
            f"Промокод: <code>{code.upper()}</code>"
        )
    else:
        result_text = (
            f"❌ <b>Ошибка активации</b>\n\n"
            f"{msg}\n\n"
            f"Промокод: <code>{code.upper()}</code>"
        )
    
    await state.clear()
    
    reply_markup = get_back_to_menu_keyboard()
    sent = False
    try:
        if bot_message_id and state_chat_id == chat_id:
            await bot.edit_message_text(
                result_text,
                chat_id=chat_id,
                message_id=bot_message_id,
                reply_markup=reply_markup
            )
            sent = True
    except Exception as e:
        logger.warning(f"Promocode edit failed: {e}")
    if not sent:
        await bot.send_message(chat_id=chat_id, text=result_text, reply_markup=reply_markup)


@dp.message(PromocodeState.waiting_for_code)
async def process_promocode_non_text(message: types.Message):
    """Handle non-text input when waiting for promocode"""
    await message.answer(
        "❌ Введите промокод текстом.\n\nНапример: <code>SOWWME</code>",
        reply_markup=get_back_to_menu_keyboard()
    )


# ===== REFERRAL HANDLERS =====

@dp.callback_query(F.data == "referral_menu")
async def referral_menu_handler(callback: types.CallbackQuery):
    """Show referral menu"""
    await callback.answer()
    user_id = callback.from_user.id
    
    # Get referral stats
    stats = db.get_referral_stats(user_id)
    
    # Generate referral link
    bot_username = (await bot.me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    
    text = (
        "👥 <b>Реферальная программа</b>\n\n"
        "🎁 Приглашайте друзей и получайте <b>3 генерации</b> за каждого!\n\n"
        "📊 <b>Ваша статистика:</b>\n"
        f"├ Приглашено: <b>{stats['total_referrals']}</b>\n"
        f"├ Получено наград: <b>{stats['claimed_rewards']}</b>\n"
        f"└ Ожидают награду: <b>{stats['pending_rewards']}</b>\n\n"
        "🔗 <b>Ваша реферальная ссылка:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        "💡 <i>Награда начисляется после первой генерации друга</i>"
    )
    
    builder = InlineKeyboardBuilder()
    
    if stats['recent_referrals']:
        builder.button(text="📋 Мои рефералы", callback_data="my_referrals")
    
    builder.button(text="🏠 В главное меню", callback_data="back_to_menu")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "my_referrals")
async def show_referrals_handler(callback: types.CallbackQuery):
    """Show user's referrals list"""
    await callback.answer()
    user_id = callback.from_user.id
    
    stats = db.get_referral_stats(user_id)
    
    text = "👥 <b>Ваши рефералы:</b>\n\n"
    
    if stats['recent_referrals']:
        for ref in stats['recent_referrals']:
            referred_id, first_name, username, claimed, created_at = ref
            status = "✅" if claimed else "⏳"
            name = first_name or username or f"ID{referred_id}"
            text += f"{status} {name}\n"
    else:
        text += "У вас пока нет рефералов"
    
    text += "\n✅ - награда получена\n⏳ - ожидает первой генерации"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="referral_menu")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ===== MORE MENU & ONBOARDING =====

@dp.callback_query(F.data == "more_menu")
async def more_menu_handler(callback: types.CallbackQuery):
    """Show additional menu options"""
    await callback.answer()
    user_id = callback.from_user.id
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🎁 Промокод", callback_data="enter_promocode")
    builder.button(text="👥 Рефералы", callback_data="referral_menu")
    builder.button(text="ℹ️ О боте", callback_data="about_bot")
    builder.button(text="🆘 Помощь", callback_data="help_support")
    
    # Add admin button if user is admin
    if user_id in ADMIN_IDS:
        builder.button(text="⚙️ Админка", callback_data="admin_panel")
    
    builder.button(text="🏠 Главное меню", callback_data="back_to_menu")
    builder.adjust(2, 2, 1)
    
    text = (
        "📋 <b>Дополнительное меню</b>\n\n"
        "Выберите действие:"
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "about_bot")
async def about_bot_handler(callback: types.CallbackQuery):
    """Show bot information and quick guide"""
    await callback.answer()
    
    text = (
        "🎨 <b>Nano Banana Bot</b>\n\n"
        "Генерация и редактирование изображений с помощью AI\n\n"
        "✨ <b>Возможности:</b>\n"
        "🎨 <b>Генерация</b> — создание изображений по текстовому описанию\n"
        "✏️ <b>Редактирование</b> — изменение ваших фото (до 4 за раз)\n"
        "⬆️ <b>Повышение качества</b> — улучшение качества любых фото до 1K/2K/4K/8K\n"
        "📐 <b>11 форматов</b> — от 1:1 до 21:9\n\n"
        "💡 <b>Пример промпта:</b>\n"
        "<i>Уютное кафе с большими окнами, мягкое вечернее освещение, деревянные столы, зелёные растения в горшках, тёплая атмосфера, реалистичный стиль, детальная прорисовка, 4k качество</i>\n\n"
        "🚀 <b>Быстрый старт:</b>\n"
        "1. Нажми 🎨 Генерация\n"
        "2. Отправь ЛЮБОЕ фото + описание в подписи\n"
        "   (или просто текст, если без фото)\n"
        "3. Выбери формат → Готово! 🎉\n\n"
        "⏱ <b>Время генерации:</b> 5-15 секунд"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🎨 Попробовать", callback_data="generate_image")
    builder.button(text="⬅️ Назад", callback_data="more_menu")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "help_support")
async def help_support_handler(callback: types.CallbackQuery):
    """Show support information"""
    await callback.answer()
    
    text = (
        "🆘 <b>Техническая поддержка</b>\n\n"
        "Если у вас возникли вопросы или проблемы с ботом, свяжитесь с нашей службой поддержки:\n\n"
        "📧 <b>Контакт:</b> @Sowwme\n\n"
        "⏰ <b>Время ответа:</b> в течение 24 часов\n"
        "⚡ Как правило, намного быстрее!\n\n"
        "💬 <b>Мы поможем с:</b>\n"
        "• Вопросами по работе бота\n"
        "• Проблемами с оплатой\n"
        "• Техническими неполадками\n"
        "• Предложениями по улучшению"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="back_to_menu")
    builder.adjust(1)
    
    try:
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    except Exception:
        await callback.message.answer(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "show_all_resolutions")
async def show_all_resolutions_handler(callback: types.CallbackQuery, state: FSMContext):
    """Show all resolution options"""
    await callback.answer()
    
    # Get current state to determine back callback
    current_state = await state.get_state()
    
    text = "📐 <b>Дополнительные форматы</b>\n\nВыберите соотношение сторон:"
    
    await callback.message.edit_text(text, reply_markup=get_resolution_keyboard(show_all=True))

@dp.callback_query(F.data == "resolution_main")
async def back_to_main_resolutions_handler(callback: types.CallbackQuery, state: FSMContext):
    """Return to main 4 resolutions"""
    await callback.answer()
    
    text = "Теперь выберите соотношение сторон:"
    
    await callback.message.edit_text(text, reply_markup=get_resolution_keyboard())

# ===== ADMIN PROMOCODE HANDLERS =====

@dp.callback_query(F.data == "admin_promocodes")
async def admin_promocodes_handler(callback: types.CallbackQuery):
    """Show admin promocodes menu"""
    await callback.answer()
    
    promos = db.get_all_promocodes()
    
    text = "🎁 <b>Управление промокодами</b>\n\n"
    
    if promos:
        text += f"📊 Всего промокодов: <b>{len(promos)}</b>\n\n"
        for promo in promos[:10]:  # Show first 10
            promo_id, code, r_type, r_value, max_uses, current_uses, expires_at, is_active, created_at = promo
            
            status = "🟢" if is_active else "🔴"
            
            if r_type == "generations":
                reward = f"{r_value} ген."
            else:
                plans = {1: "MINI", 2: "STARTER", 3: "PRO", 4: "UNLIMITED"}
                reward = plans.get(r_value, "подписка")
            
            uses_text = f"{current_uses}/{max_uses}" if max_uses > 0 else f"{current_uses}/∞"
            
            text += f"{status} <code>{code}</code> - {reward} [{uses_text}]\n"
    else:
        text += "Промокодов пока нет"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Создать промокод", callback_data="admin_create_promo")
    
    if promos:
        builder.button(text="🗑 Удалить промокод", callback_data="admin_delete_promo")
        builder.button(text="📋 Все промокоды", callback_data="admin_all_promos")
    
    builder.button(text="⬅️ Назад в админку", callback_data="admin_panel")
    builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# --- Promocode creation form ---

def _promo_back_kb():
    b = InlineKeyboardBuilder()
    b.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    return b.as_markup()

@dp.callback_query(F.data == "admin_create_promo")
async def admin_create_promo_handler(callback: types.CallbackQuery, state: FSMContext):
    """Start promocode creation - step 1: enter code"""
    await callback.answer()
    await state.clear()
    await state.set_state(AdminState.create_promo_code)
    
    await callback.message.edit_text(
        "➕ <b>Создание промокода</b>\n\n"
        "Шаг 1/5: Введите код промокода\n\n"
        "Например: <code>WELCOME10</code> или <code>VIP2024</code>",
        reply_markup=_promo_back_kb()
    )

@dp.message(AdminState.create_promo_code)
async def create_promo_step1_code(message: types.Message, state: FSMContext):
    """Save code, show type selection"""
    if not message.text:
        await message.answer("❌ Введите код промокода текстом", reply_markup=_promo_back_kb())
        return
    try:
        await message.delete()
    except Exception:
        pass
    code = message.text.strip().upper()
    if not code or len(code) < 2:
        await message.answer("❌ Введите код (минимум 2 символа)", reply_markup=_promo_back_kb())
        return
    
    await state.update_data(create_promo_code=code)
    await state.set_state(None)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🎨 Генерации", callback_data="create_promo_type_generations")
    builder.button(text="📦 Подписка", callback_data="create_promo_type_subscription")
    builder.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    builder.adjust(2, 1)
    
    await message.answer(
        f"➕ <b>Создание промокода</b>\n\n"
        f"Код: <code>{code}</code>\n\n"
        "Шаг 2/5: Выберите тип награды",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data == "create_promo_type_generations")
@dp.callback_query(F.data == "create_promo_type_subscription")
async def create_promo_step2_type(callback: types.CallbackQuery, state: FSMContext):
    """Save type, show value selection"""
    await callback.answer()
    r_type = "generations" if "generations" in callback.data else "subscription"
    await state.update_data(create_promo_type=r_type)
    
    data = await state.get_data()
    code = data.get("create_promo_code", "")
    
    builder = InlineKeyboardBuilder()
    if r_type == "generations":
        for n in [5, 10, 15, 20]:
            builder.button(text=str(n), callback_data=f"create_promo_val_{n}")
        builder.button(text="✏️ Своё значение", callback_data="create_promo_val_custom")
    else:
        builder.button(text="🌟 MINI (5 ген)", callback_data="create_promo_val_1")
        builder.button(text="🟢 STARTER (10)", callback_data="create_promo_val_2")
        builder.button(text="🔵 PRO (30)", callback_data="create_promo_val_3")
        builder.button(text="⭐ UNLIMITED (90)", callback_data="create_promo_val_4")
    builder.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    builder.adjust(3, 3, 1)
    
    type_text = "генерации" if r_type == "generations" else "подписка"
    await callback.message.edit_text(
        f"➕ <b>Создание промокода</b>\n\n"
        f"Код: <code>{code}</code>\n"
        f"Тип: {type_text}\n\n"
        "Шаг 3/5: Выберите значение",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data == "create_promo_val_custom")
async def create_promo_step3_value_custom(callback: types.CallbackQuery, state: FSMContext):
    """Ask for custom generations value"""
    await callback.answer()
    data = await state.get_data()
    if data.get("create_promo_type") != "generations":
        return
    await state.set_state(AdminState.create_promo_value_custom)
    await callback.message.edit_text(
        "✏️ Введите число генераций (1–999):",
        reply_markup=_promo_back_kb()
    )

@dp.message(AdminState.create_promo_value_custom, F.text)
async def create_promo_value_custom_handler(message: types.Message, state: FSMContext):
    """Process custom generations value"""
    try:
        await message.delete()
    except Exception:
        pass
    try:
        val = int(message.text.strip())
        if val < 1 or val > 999:
            raise ValueError("Число должно быть от 1 до 999")
    except (ValueError, TypeError):
        await message.answer("❌ Введите целое число от 1 до 999", reply_markup=_promo_back_kb())
        return
    await state.update_data(create_promo_value=val)
    await state.set_state(None)
    data = await state.get_data()
    code = data.get("create_promo_code", "")
    r_type = data.get("create_promo_type", "")
    builder = InlineKeyboardBuilder()
    builder.button(text="∞ Безлимит", callback_data="create_promo_max_0")
    for n in [1, 5, 10, 50, 100]:
        builder.button(text=str(n), callback_data=f"create_promo_max_{n}")
    builder.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    builder.adjust(3, 3, 1)
    await message.answer(
        f"➕ <b>Создание промокода</b>\n\n"
        f"Код: <code>{code}</code>\n"
        f"Тип: генерации\n"
        f"Значение: {val} ген.\n\n"
        "Шаг 4/5: Макс. использований",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("create_promo_val_"))
async def create_promo_step3_value(callback: types.CallbackQuery, state: FSMContext):
    """Save value, show max uses"""
    await callback.answer()
    val = callback.data.replace("create_promo_val_", "")
    if val == "custom":
        return
    r_value = int(val)
    await state.update_data(create_promo_value=r_value)
    
    data = await state.get_data()
    code = data.get("create_promo_code", "")
    r_type = data.get("create_promo_type", "")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="∞ Безлимит", callback_data="create_promo_max_0")
    for n in [1, 5, 10, 50, 100]:
        builder.button(text=str(n), callback_data=f"create_promo_max_{n}")
    builder.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    builder.adjust(3, 3, 1)
    
    val_text = f"{r_value} ген." if r_type == "generations" else {1: "MINI", 2: "STARTER", 3: "PRO", 4: "UNLIMITED"}.get(r_value, str(r_value))
    
    await callback.message.edit_text(
        f"➕ <b>Создание промокода</b>\n\n"
        f"Код: <code>{code}</code>\n"
        f"Тип: {'генерации' if r_type == 'generations' else 'подписка'}\n"
        f"Значение: {val_text}\n\n"
        "Шаг 4/5: Макс. использований",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("create_promo_max_"))
async def create_promo_step4_max(callback: types.CallbackQuery, state: FSMContext):
    """Save max uses, show days"""
    await callback.answer()
    max_val = callback.data.replace("create_promo_max_", "")
    max_uses = int(max_val)
    await state.update_data(create_promo_max=max_uses)
    
    data = await state.get_data()
    code = data.get("create_promo_code", "")
    r_type = data.get("create_promo_type", "")
    r_value = data.get("create_promo_value", 0)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="∞ Бессрочно", callback_data="create_promo_days_0")
    for n in [7, 30, 60, 90]:
        builder.button(text=f"{n} дн.", callback_data=f"create_promo_days_{n}")
    builder.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    builder.adjust(2, 2, 1)
    
    max_text = "∞" if max_uses == 0 else str(max_uses)
    
    await callback.message.edit_text(
        f"➕ <b>Создание промокода</b>\n\n"
        f"Код: <code>{code}</code>\n"
        f"Тип: {'генерации' if r_type == 'generations' else 'подписка'}\n"
        f"Значение: {r_value}\n"
        f"Макс. использований: {max_text}\n\n"
        "Шаг 5/5: Срок действия",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data.startswith("create_promo_days_"))
async def create_promo_step5_days(callback: types.CallbackQuery, state: FSMContext):
    """Save days, show confirm and create"""
    await callback.answer()
    days_val = callback.data.replace("create_promo_days_", "")
    days = int(days_val)
    await state.update_data(create_promo_days=days)
    
    data = await state.get_data()
    code = data.get("create_promo_code", "")
    r_type = data.get("create_promo_type", "")
    r_value = data.get("create_promo_value", 0)
    
    max_text = "∞" if data.get("create_promo_max", 0) == 0 else str(data.get("create_promo_max"))
    days_text = "∞" if days == 0 else f"{days} дн."
    val_text = f"{r_value} ген." if r_type == "generations" else {1: "MINI", 2: "STARTER", 3: "PRO", 4: "UNLIMITED"}.get(r_value, str(r_value))
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Создать", callback_data="create_promo_confirm_ok")
    builder.button(text="❌ Отмена", callback_data="admin_create_promo_cancel")
    builder.adjust(1)
    
    await callback.message.edit_text(
        f"➕ <b>Подтверждение</b>\n\n"
        f"Код: <code>{code}</code>\n"
        f"Тип: {'генерации' if r_type == 'generations' else 'подписка'}\n"
        f"Значение: {val_text}\n"
        f"Макс. использований: {max_text}\n"
        f"Срок: {days_text}\n\n"
        "Создать промокод?",
        reply_markup=builder.as_markup()
    )

@dp.callback_query(F.data == "create_promo_confirm_ok")
async def create_promo_confirm_handler(callback: types.CallbackQuery, state: FSMContext):
    """Create promocode and show result"""
    await callback.answer()
    data = await state.get_data()
    code = data.get("create_promo_code", "")
    r_type = data.get("create_promo_type", "")
    r_value = data.get("create_promo_value", 0)
    max_uses = data.get("create_promo_max", 0)
    days = data.get("create_promo_days", 30)
    
    success, msg = db.create_promocode(code, r_type, r_value, max_uses, days)
    
    await state.clear()
    
    if success:
        text = f"✅ {msg}"
    else:
        text = f"❌ {msg}"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ К промокодам", callback_data="admin_promocodes")
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

@dp.callback_query(F.data == "admin_create_promo_cancel")
async def create_promo_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Cancel promocode creation"""
    await callback.answer()
    await state.clear()
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ К промокодам", callback_data="admin_promocodes")
    await callback.message.edit_text("❌ Создание отменено", reply_markup=builder.as_markup())

def get_promocode_delete_keyboard():
    """Keyboard with promocodes and delete buttons"""
    promos = db.get_all_promocodes()
    builder = InlineKeyboardBuilder()
    for promo in promos:
        promo_id, code, r_type, r_value, max_uses, current_uses, _, is_active, _ = promo
        status = "🟢" if is_active else "🔴"
        if r_type == "generations":
            reward = f"{r_value} ген."
        else:
            plans = {1: "MINI", 2: "STARTER", 3: "PRO", 4: "UNLIMITED"}
            reward = plans.get(r_value, "подписка")
        btn_text = f"{status} {code} ({reward}) — удалить"
        # callback_data max 64 bytes, use promo_id
        builder.button(text=btn_text, callback_data=f"admin_del_{promo_id}")
    builder.button(text="⬅️ Назад", callback_data="admin_promocodes")
    builder.adjust(1)
    return builder.as_markup()


@dp.callback_query(F.data == "admin_delete_promo")
async def admin_delete_promo_handler(callback: types.CallbackQuery):
    """Show promocodes with delete buttons"""
    await callback.answer()
    
    promos = db.get_all_promocodes()
    if not promos:
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data="admin_promocodes")
        await callback.message.edit_text(
            "🗑 <b>Удаление промокода</b>\n\nПромокодов пока нет.",
            reply_markup=builder.as_markup()
        )
        return
    
    text = "🗑 <b>Удаление промокода</b>\n\nВыберите промокод для удаления:"
    await callback.message.edit_text(text, reply_markup=get_promocode_delete_keyboard())


@dp.callback_query(F.data.startswith("admin_del_"))
async def admin_delete_promo_confirm_handler(callback: types.CallbackQuery):
    """Delete promocode by ID from button click"""
    await callback.answer()
    try:
        promo_id = int(callback.data.replace("admin_del_", ""))
    except ValueError:
        await callback.answer("❌ Ошибка", show_alert=True)
        return
    
    promos = db.get_all_promocodes()
    code_to_delete = None
    for promo in promos:
        if promo[0] == promo_id:
            code_to_delete = promo[1]
            break
    
    if not code_to_delete:
        await callback.answer("❌ Промокод не найден", show_alert=True)
        return
    
    success = db.delete_promocode(code_to_delete)
    if success:
        await callback.answer(f"✅ {code_to_delete} удалён", show_alert=True)
        promos = db.get_all_promocodes()
        if promos:
            text = "🗑 <b>Удаление промокода</b>\n\nВыберите промокод для удаления:"
            await callback.message.edit_text(text, reply_markup=get_promocode_delete_keyboard())
        else:
            builder = InlineKeyboardBuilder()
            builder.button(text="⬅️ Назад", callback_data="admin_promocodes")
            await callback.message.edit_text(
                "🗑 <b>Удаление промокода</b>\n\n✅ Промокод удалён.\nПромокодов больше нет.",
                reply_markup=builder.as_markup()
            )
    else:
        await callback.answer("❌ Не удалось удалить", show_alert=True)

@dp.callback_query(F.data == "admin_all_promos")
async def admin_all_promos_handler(callback: types.CallbackQuery):
    """Show all promocodes"""
    await callback.answer()
    
    promos = db.get_all_promocodes()
    
    text = "📋 <b>Все промокоды:</b>\n\n"
    
    for promo in promos:
        promo_id, code, r_type, r_value, max_uses, current_uses, expires_at, is_active, created_at = promo
        
        status = "🟢" if is_active else "🔴"
        
        if r_type == "generations":
            reward = f"{r_value} генераций"
        else:
            plans = {1: "MINI", 2: "STARTER", 3: "PRO", 4: "UNLIMITED"}
            reward = f"Подписка {plans.get(r_value, '?')}"
        
        uses_text = f"{current_uses}/{max_uses}" if max_uses > 0 else f"{current_uses}/∞"
        
        # Format expiry
        if expires_at:
            try:
                exp_dt = datetime.fromisoformat(expires_at)
                exp_str = exp_dt.strftime("%d.%m.%Y")
            except:
                exp_str = expires_at[:10]
        else:
            exp_str = "∞"
        
        text += (
            f"{status} <b>{code}</b>\n"
            f"├ Награда: {reward}\n"
            f"├ Использовано: {uses_text}\n"
            f"└ Истекает: {exp_str}\n\n"
        )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="admin_promocodes")
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup())

# ===== IMAGE UPSCALE HANDLERS =====

@dp.callback_query(F.data == "upscale_image")
async def upscale_image_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle upscale image button"""
    await callback.answer()
    
    user_id = callback.from_user.id
    logger.info(f"Upscale button clicked by user {user_id}")
    
    # Check generations first (for all users)
    can_generate, msg = db.can_generate(user_id)
    if not can_generate:
        if await should_show_channel_notification(user_id):
            await callback.message.edit_text(
                "⛔ <b>У вас нет доступных генераций</b>\n\n"
                "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                "Там мы делимся лучшими промптами, примерами работ и новостями.",
                reply_markup=get_channel_keyboard(no_generations=True)
            )
        else:
            await callback.message.edit_text(
                f"⛔ <b>У вас нет доступных генераций</b>\n\n{msg}\n\n"
                "💎 Перейдите в магазин, чтобы купить подписку",
                reply_markup=get_main_menu_keyboard(user_id)
            )
        return
    
    # If channel notification needed — show CTA first, then ask for image (same as generate)
    if await should_show_channel_notification(user_id):
        await state.set_state(UpscaleImageState.waiting_for_image)
        await callback.message.edit_text(
            "🎁 <b>Вы можете получить дополнительную 1 генерацию за подписку на канал</b>\n\n"
            "Там мы делимся лучшими промптами, примерами работ и новостями.",
            reply_markup=get_channel_keyboard(from_upscale=True)
        )
        return
    
    # Always ask to upload new image
    await state.set_state(UpscaleImageState.waiting_for_image)
    
    try:
        await callback.message.edit_text(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())
    except Exception:
        await callback.message.answer(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "upscale_new_image")
async def upscale_new_image_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle upload new image for upscale"""
    await callback.answer()
    
    await state.set_state(UpscaleImageState.waiting_for_image)
    
    try:
        await callback.message.edit_text(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())
    except Exception:
        await callback.message.answer(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(F.data == "upscale_this_image")
async def upscale_this_image_callback(callback: types.CallbackQuery, state: FSMContext):
    """Handle upscale button for just generated image"""
    await callback.answer()
    
    user_id = callback.from_user.id
    
    # Check generations first (upscale requires at least 1 gen)
    can_generate, msg = db.can_generate(user_id)
    if not can_generate:
        if await should_show_channel_notification(user_id):
            await callback.message.edit_text(
                "⛔ <b>У вас нет доступных генераций</b>\n\n"
                "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                "Там мы делимся лучшими промптами, примерами работ и новостями.",
                reply_markup=get_channel_keyboard(no_generations=True)
            )
        else:
            await callback.message.edit_text(
                f"⛔ <b>У вас нет доступных генераций</b>\n\n{msg}\n\n"
                "💎 Перейдите в магазин, чтобы купить подписку",
                reply_markup=get_main_menu_keyboard(user_id)
            )
        return
    
    # Get saved file_id from state first, then from database
    data = await state.get_data()
    file_id = data.get("last_generated_image_file_id")
    
    if not file_id:
        # Try to get from database
        file_id = db.get_last_generated_image(user_id)
    
    if not file_id:
        await callback.message.answer(
            "❌ Изображение не найдено. Попробуйте сгенерировать новое изображение.",
            reply_markup=get_main_menu_keyboard(user_id)
        )
        return
    
    # Save file_id for upscale process
    await state.update_data(photo_file_id=file_id)
    
    # Show upscale factor selection
    text = (
        "⬆️ <b>Выберите уровень улучшения:</b>\n\n"
        "Чем выше множитель, тем лучше качество и больше разрешение результата.\n\n"
        "💡 Рекомендации:\n"
        "• 1K/2K - для небольших улучшений\n"
        "• 4K - оптимальный вариант для большинства задач\n"
        "• 8K - максимальное качество для печати"
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=get_upscale_factor_keyboard())
    except Exception:
        await callback.message.answer(text, reply_markup=get_upscale_factor_keyboard())
    
    await state.set_state(UpscaleImageState.waiting_for_factor)

@dp.message(UpscaleImageState.waiting_for_image, F.photo)
async def upscale_receive_image(message: types.Message, state: FSMContext):
    """Handle received image for upscaling"""
    await message.answer("📥 Изображение получено! Выберите уровень улучшения:")
    
    # Save image info to state
    photo = message.photo[-1]  # Get highest resolution
    await state.update_data(photo_file_id=photo.file_id)
    
    # Show upscale factor selection
    text = (
        "⬆️ <b>Выберите уровень улучшения:</b>\n\n"
        "Чем выше множитель, тем лучше качество и больше разрешение результата.\n\n"
        "💡 Рекомендации:\n"
        "• 1K/2K - для небольших улучшений\n"
        "• 4K - оптимальный вариант для большинства задач\n"
        "• 8K - максимальное качество для печати"
    )
    
    await message.answer(text, reply_markup=get_upscale_factor_keyboard(show_back=True))
    await state.set_state(UpscaleImageState.waiting_for_factor)

@dp.message(UpscaleImageState.waiting_for_image)
async def upscale_invalid_input(message: types.Message):
    """Handle invalid input during image waiting"""
    await message.answer(
        "⚠️ Пожалуйста, отправьте изображение.\n\n"
        "Если хотите вернуться в меню, нажмите кнопку ниже.",
        reply_markup=get_back_to_menu_keyboard()
    )

@dp.callback_query(F.data == "back_to_upscale_image")
async def back_to_upscale_image_callback(callback: types.CallbackQuery, state: FSMContext):
    """Return to image upload for upscale"""
    await callback.answer()
    
    await state.set_state(UpscaleImageState.waiting_for_image)
    
    try:
        await callback.message.edit_text(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())
    except Exception:
        await callback.message.answer(UPSCALE_IMAGE_TEXT, reply_markup=get_back_to_menu_keyboard())

@dp.callback_query(UpscaleImageState.waiting_for_factor, F.data.startswith("upscale_factor_"))
async def upscale_process(callback: types.CallbackQuery, state: FSMContext):
    """Process upscale with selected factor"""
    await callback.answer()
    
    # Delete the menu message (like inline keyboard)
    try:
        await callback.message.delete()
    except Exception:
        pass
    
    # Extract factor from callback
    factor = callback.data.split("_")[-1]
    
    # Get saved data
    data = await state.get_data()
    photo_file_id = data.get("photo_file_id")
    
    if not photo_file_id:
        await callback.message.answer("❌ Ошибка: изображение не найдено. Попробуйте еще раз.")
        await state.clear()
        return
    
    # Check if user has enough generations
    user_id = callback.from_user.id
    upscale_cost = UPSCALE_FACTORS[factor]["cost"]
    
    # Get user info to check balance
    user_info = db.get_user_info(user_id)
    
    # Admins have unlimited access
    if not user_info["is_admin"]:
        if user_info["generations_left"] < upscale_cost:
            cost_text = f"{upscale_cost} генераций" if upscale_cost > 1 else f"{upscale_cost} генерация"
            left_text = f"{user_info['generations_left']:.1f}" if user_info['generations_left'] != int(user_info['generations_left']) else f"{int(user_info['generations_left'])}"
            if await should_show_channel_notification(user_id):
                text = (
                    f"⛔ <b>У вас нет доступных генераций</b>\n\n"
                    f"Для улучшения {UPSCALE_FACTORS[factor]['name']} нужно <b>{cost_text}</b>. "
                    f"У вас осталось: <b>{left_text} ген.</b>\n\n"
                    "🎁 Но вы можете подписаться на канал и получить +1 генерацию бесплатно!\n\n"
                    "Там мы делимся лучшими промптами, примерами работ и новостями."
                )
                kb = get_channel_keyboard(no_generations=True)
            else:
                text = (
                    f"⛔ <b>У вас нет доступных генераций</b>\n\n"
                    f"Для улучшения {UPSCALE_FACTORS[factor]['name']} нужно <b>{cost_text}</b>\n"
                    f"У вас осталось: <b>{left_text} ген.</b>\n\n"
                    "💎 Перейдите в магазин для покупки генераций."
                )
                kb = get_back_to_menu_keyboard()
            await bot.send_message(callback.message.chat.id, text, reply_markup=kb)
            await state.clear()
            return
    
    # Show processing message
    factor_name = UPSCALE_FACTORS[factor]["name"]
    processing_msg = await bot.send_message(
        callback.message.chat.id,
        f"⏳ <b>Улучшаем изображение до {factor_name}...</b>\n\n"
        "Это может занять до 1-2 минут в зависимости от размера."
    )
    
    try:
        # Скачиваем фото и заливаем на публичный хостинг — API не может качать по ссылке Telegram
        file = await bot.get_file(photo_file_id)
        file_path = file.file_path
        file_bytes = await bot.download_file(file_path)
        image_bytes = file_bytes.read() if hasattr(file_bytes, "read") else file_bytes
        if not isinstance(image_bytes, bytes):
            image_bytes = getattr(file_bytes, "getvalue", lambda: b"")() or b""

        # Конвертируем в JPEG — kie.ai API возвращает "Image format error" на WebP/HEIC от Telegram
        # kie.ai: longest_side × upscale_factor ≤ 20000 → max_side = 20000/factor
        max_side_by_factor = {"1": 20000, "2": 10000, "4": 5000, "8": 2500}
        max_side = max_side_by_factor.get(factor, 4096)
        image_bytes = ensure_jpeg_for_api(image_bytes, max_side=max_side) or image_bytes

        temp_url = await upload_image_to_temporary_host(image_bytes)
        if not temp_url:
            await processing_msg.edit_text(
                "❌ <b>Не удалось загрузить изображение</b>\n\nПопробуйте другое фото или позже.",
                reply_markup=get_back_to_menu_keyboard()
            )
            await state.clear()
            return

        logger.info(f"Upscaling image: factor={factor}, public URL")
        try:
            result_image = await asyncio.wait_for(
                upscale_image_via_api(temp_url, factor),
                timeout=180.0
            )
        except asyncio.TimeoutError:
            logger.error("❌ Upscale timeout")
            result_image = None

        if result_image:
            # Record usage with custom cost
            db.use_generation(user_id, f"upscale_{factor}x", "upscale", cost=upscale_cost)
            
            # Get user info
            info = db.get_user_info(user_id)
            
            # Build caption with generation info
            caption = f"✅ <b>Изображение улучшено до {factor_name}!</b>"
            if not info["is_admin"]:
                gens_left = info['generations_left']
                gens_text = f"{int(gens_left)}" if gens_left == int(gens_left) else f"{gens_left:.1f}"
                caption += f"\n\n📊 Осталось генераций: <b>{gens_text}</b>"
            
            # Send result with main menu button
            await bot.send_photo(
                callback.message.chat.id,
                types.BufferedInputFile(result_image, filename="upscaled.jpg"),
                caption=caption,
                reply_markup=get_back_to_menu_keyboard()
            )
            
            # Channel CTA after upscale (same rules)
            if await should_show_channel_after_generation(user_id):
                await bot.send_message(
                    callback.message.chat.id,
                    "🎁 Подпишись на канал и получи ещё +1 генерацию!",
                    reply_markup=get_channel_keyboard()
                )
            
            await processing_msg.delete()
        else:
            await processing_msg.edit_text(
                "❌ <b>Ошибка улучшения изображения</b>\n\n"
                "Попробуйте:\n"
                "• Другое фото (JPEG/PNG)\n"
                "• Уменьшить размер изображения\n"
                "• Нажать «В главное меню» и повторить\n\n"
                "Если не помогает — обратитесь в поддержку.",
                reply_markup=get_back_to_menu_keyboard()
            )
    
    except Exception as e:
        logger.error(f"Error in upscale process: {e}")
        await processing_msg.edit_text(
            "❌ <b>Произошла ошибка</b>\n\n"
            "Попробуйте еще раз позже.",
            reply_markup=get_back_to_menu_keyboard()
        )
    
    await state.clear()

async def main():
    """Main function to start the bot and web API server"""
    logger.info("Starting Nano Banana Telegram Bot...")

    # Start web API server for mini app
    from web_api import start_web_api
    try:
        api_runner = await start_web_api()
        logger.info("Web API server started alongside bot")
    except Exception as e:
        logger.error(f"Failed to start Web API: {e}")
        api_runner = None

    # Start polling
    try:
        await dp.start_polling(bot)
    finally:
        if api_runner:
            await api_runner.cleanup()

if __name__ == "__main__":
    import sys
    import psutil
    
    # Check for duplicate bot processes
    current_pid = os.getpid()
    bot_processes = []
    
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if cmdline and 'telegram_bot.py' in ' '.join(cmdline) and proc.info['pid'] != current_pid:
                    bot_processes.append(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception as e:
        logger.warning(f"Could not check for duplicate processes: {e}")
    
    if bot_processes:
        logger.warning(f"WARNING: Found {len(bot_processes)} other bot instance(s) running: {bot_processes}")
        logger.warning("WARNING: Multiple bot instances may cause conflicts!")
        logger.warning("WARNING: Please stop other instances before starting a new one.")
        print(f"\nWARNING: Found {len(bot_processes)} other bot instance(s) already running!")
        print(f"PIDs: {bot_processes}")
        print(f"This may cause Telegram API conflicts.")
        # Не блокировать при запуске в фоне (без интерактивного ввода)
        try:
            import sys
            if sys.stdin.isatty():
                print(f"\nContinue anyway? (y/n): ", end='')
                response = input().strip().lower()
                if response != 'y':
                    print("Exiting...")
                    sys.exit(0)
            else:
                print("(Non-interactive mode: continuing anyway)")
        except (EOFError, KeyboardInterrupt):
            pass
    
    asyncio.run(main())