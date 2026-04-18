import asyncio
import logging
import os
import signal
from dataclasses import dataclass
from typing import Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

OWNER_IDS = {
    int(x.strip())
    for x in os.getenv("OWNER_IDS", "").split(",")
    if x.strip().isdigit()
}

DEFAULT_TARGET_CHAT_RAW = os.getenv("DEFAULT_TARGET_CHAT", "").strip()
DEFAULT_SEND_DELAY = int(os.getenv("DEFAULT_SEND_DELAY", "5"))
MAX_SEND_DELAY = int(os.getenv("MAX_SEND_DELAY", "30"))

CATCHER_INLINE_BOT = os.getenv("CATCHER_INLINE_BOT", "@Character_Catcher_Bot").strip()
SEIZER_INLINE_BOT = os.getenv("SEIZER_INLINE_BOT", "@Character_Seizer_Bot").strip()

USE_WEBHOOK = os.getenv("USE_WEBHOOK", "false").lower() == "true"
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip() or "/webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
PORT = int(os.getenv("PORT", "8080"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not API_ID:
    raise RuntimeError("API_ID is required")
if not API_HASH:
    raise RuntimeError("API_HASH is required")
if not SESSION_STRING:
    raise RuntimeError("SESSION_STRING is required")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS is required")
if not DEFAULT_TARGET_CHAT_RAW:
    raise RuntimeError("DEFAULT_TARGET_CHAT is required")
if USE_WEBHOOK and (not PUBLIC_URL or not WEBHOOK_SECRET):
    raise RuntimeError("PUBLIC_URL and WEBHOOK_SECRET are required when USE_WEBHOOK=true")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("inline-seeder")

router = Router()
dp = Dispatcher()
dp.include_router(router)

bot: Optional[Bot] = None
user_client: Optional[Client] = None


def clean_value(value: str) -> str:
    return " ".join((value or "").split()).strip()


def html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def normalize_webhook_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"


def parse_chat_ref(value: str):
    value = clean_value(value)
    if value.lstrip("-").isdigit():
        return int(value)
    return value


DEFAULT_TARGET_CHAT = parse_chat_ref(DEFAULT_TARGET_CHAT_RAW)


def is_owner(message: Message) -> bool:
    return bool(message.from_user and message.from_user.id in OWNER_IDS)


def is_target_chat(message: Message) -> bool:
    return bool(message.chat and message.chat.id == DEFAULT_TARGET_CHAT)


async def require_owner_in_target_chat(message: Message) -> bool:
    if not is_target_chat(message):
        return False
    if not is_owner(message):
        return False
    return True


@dataclass
class RunnerState:
    running: bool = False
    sent_count: int = 0
    delay_seconds: int = DEFAULT_SEND_DELAY
    target_chat: str | int = DEFAULT_TARGET_CHAT
    current_offset: str = ""
    last_error: str = ""
    source_bot: str = ""


class InlineSeeder:
    def __init__(self, client: Client):
        self.client = client
        self.task: Optional[asyncio.Task] = None
        self.stop_event = asyncio.Event()
        self.state = RunnerState()

    def is_running(self) -> bool:
        return self.task is not None and not self.task.done()

    async def start(self, source_bot: str, delay_seconds: int):
        if self.is_running():
            raise RuntimeError("Seeder is already running")

        delay_seconds = max(1, min(delay_seconds, MAX_SEND_DELAY))
        self.stop_event = asyncio.Event()
        self.state = RunnerState(
            running=True,
            sent_count=0,
            delay_seconds=delay_seconds,
            target_chat=DEFAULT_TARGET_CHAT,
            current_offset="",
            last_error="",
            source_bot=source_bot,
        )
        self.task = asyncio.create_task(self._worker(source_bot))

    async def stop(self):
        if not self.is_running():
            return False

        self.stop_event.set()
        try:
            await self.task
        except Exception:
            logger.exception("Seeder task stopped with error")
        finally:
            self.task = None
            self.state.running = False
        return True

    async def _worker(self, source_bot: str):
        offset = ""

        try:
            while not self.stop_event.is_set():
                results = await self.client.get_inline_bot_results(
                    bot=source_bot,
                    query="",
                    offset=offset,
                )

                if not results.results:
                    logger.info("No more inline results from %s", source_bot)
                    break

                logger.info(
                    "Fetched %s results from %s | offset=%r | next_offset=%r",
                    len(results.results),
                    source_bot,
                    offset,
                    results.next_offset,
                )

                for result in results.results:
                    if self.stop_event.is_set():
                        break

                    while True:
                        try:
                            await self.client.send_inline_bot_result(
                                chat_id=DEFAULT_TARGET_CHAT,
                                query_id=results.query_id,
                                result_id=result.id,
                            )
                            self.state.sent_count += 1
                            logger.info(
                                "Sent #%s result_id=%s from %s to %r",
                                self.state.sent_count,
                                result.id,
                                source_bot,
                                DEFAULT_TARGET_CHAT,
                            )
                            break

                        except FloodWait as e:
                            wait_for = int(getattr(e, "value", getattr(e, "x", 5)))
                            logger.warning("FloodWait %ss", wait_for)
                            await asyncio.sleep(wait_for + 1)

                        except RPCError as e:
                            self.state.last_error = str(e)
                            logger.exception("RPCError while sending inline result")
                            raise

                    await asyncio.sleep(self.state.delay_seconds)

                if self.stop_event.is_set():
                    break

                offset = results.next_offset or ""
                self.state.current_offset = offset

                if not offset:
                    logger.info("Reached final page for %s", source_bot)
                    break

        except Exception as e:
            self.state.last_error = str(e)
            logger.exception("Seeder worker failed")
        finally:
            self.state.running = False
            logger.info("Seeder worker stopped")


SEEDER: Optional[InlineSeeder] = None


@router.message(Command("start"))
async def start_handler(message: Message):
    if not await require_owner_in_target_chat(message):
        return

    mode = "webhook" if USE_WEBHOOK else "polling"
    await message.reply(
        "Inline Seeder ready.\n\n"
        f"Mode: <b>{mode}</b>\n"
        f"Target chat: <code>{html_escape(str(DEFAULT_TARGET_CHAT))}</code>\n"
        f"Catcher: <code>{html_escape(CATCHER_INLINE_BOT)}</code>\n"
        f"Seizer: <code>{html_escape(SEIZER_INLINE_BOT)}</code>\n"
        f"Default delay: <code>{DEFAULT_SEND_DELAY}</code>s\n\n"
        "Commands:\n"
        "/startcatcherbot\n"
        "/startcatcherbot 5\n"
        "/startseizerbot\n"
        "/startseizerbot 10\n"
        "/stopinlinebot\n"
        "/status",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("status"))
async def status_handler(message: Message):
    if not await require_owner_in_target_chat(message):
        return

    me = await user_client.get_me()
    state = SEEDER.state
    await message.reply(
        f"Running: <b>{'YES' if SEEDER.is_running() else 'NO'}</b>\n"
        f"User session: <code>{html_escape(me.first_name or '')}</code> (<code>{me.id}</code>)\n"
        f"Source bot: <code>{html_escape(state.source_bot or '-')}</code>\n"
        f"Target chat: <code>{html_escape(str(state.target_chat))}</code>\n"
        f"Delay: <code>{state.delay_seconds}</code>s\n"
        f"Sent count: <code>{state.sent_count}</code>\n"
        f"Current offset: <code>{html_escape(state.current_offset or '-')}</code>\n"
        f"Last error: <code>{html_escape(state.last_error or '-')}</code>",
        parse_mode=ParseMode.HTML,
    )


async def _start_seed_from_command(message: Message, command: CommandObject, source_bot: str):
    if not await require_owner_in_target_chat(message):
        return

    raw = clean_value(command.args or "")
    delay_seconds = DEFAULT_SEND_DELAY

    if raw:
        if not raw.isdigit():
            await message.reply("Usage:\n/startcatcherbot\n/startcatcherbot 5\n/startseizerbot\n/startseizerbot 10")
            return
        delay_seconds = int(raw)

    try:
        await SEEDER.start(source_bot=source_bot, delay_seconds=delay_seconds)
        await message.reply(
            f"Started.\n"
            f"Source bot: <code>{html_escape(source_bot)}</code>\n"
            f"Target chat: <code>{html_escape(str(DEFAULT_TARGET_CHAT))}</code>\n"
            f"Delay: <code>{delay_seconds}</code>s",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await message.reply(f"Error: {html_escape(str(e))}", parse_mode=ParseMode.HTML)


@router.message(Command("startcatcherbot"))
async def startcatcherbot_handler(message: Message, command: CommandObject):
    await _start_seed_from_command(message, command, CATCHER_INLINE_BOT)


@router.message(Command("startseizerbot"))
async def startseizerbot_handler(message: Message, command: CommandObject):
    await _start_seed_from_command(message, command, SEIZER_INLINE_BOT)


@router.message(Command("stopinlinebot"))
async def stopinlinebot_handler(message: Message):
    if not await require_owner_in_target_chat(message):
        return

    stopped = await SEEDER.stop()
    if not stopped:
        await message.reply("Seeder is not running.")
        return

    await message.reply(
        f"Stopped.\nSent count: <code>{SEEDER.state.sent_count}</code>",
        parse_mode=ParseMode.HTML,
    )


async def healthz(_request: web.Request):
    return web.json_response(
        {
            "ok": True,
            "running": SEEDER.is_running(),
            "sent_count": SEEDER.state.sent_count,
            "target_chat": str(DEFAULT_TARGET_CHAT),
            "source_bot": SEEDER.state.source_bot,
        }
    )


async def start_http_server(dp: Dispatcher, bot: Bot):
    app = web.Application()
    app.router.add_get("/", healthz)
    app.router.add_get("/healthz", healthz)

    if USE_WEBHOOK:
        webhook_path = normalize_webhook_path(WEBHOOK_PATH)
        SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=WEBHOOK_SECRET,
        ).register(app, path=webhook_path)
        setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info("HTTP server started on port %s", PORT)

    if USE_WEBHOOK:
        webhook_url = f"{PUBLIC_URL.rstrip('/')}{normalize_webhook_path(WEBHOOK_PATH)}"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=False,
        )
        logger.info("Webhook set to %s", webhook_url)

    return runner


async def main():
    global bot, user_client, SEEDER

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    user_client = Client(
        name="inline_seeder_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STRING,
        in_memory=True,
    )
    await user_client.start()

    SEEDER = InlineSeeder(user_client)

    me = await user_client.get_me()
    logger.info("User session started as %s (%s)", me.first_name, me.id)

    runner = await start_http_server(dp, bot)

    stop_event = asyncio.Event()

    def _stop(*_args):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    try:
        if USE_WEBHOOK:
            logger.info("Running in webhook mode")
            await stop_event.wait()
        else:
            logger.info("Running in polling mode")
            await bot.delete_webhook(drop_pending_updates=False)
            polling_task = asyncio.create_task(
                dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
            )
            stop_task = asyncio.create_task(stop_event.wait())
            done, pending = await asyncio.wait(
                {polling_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if polling_task in done:
                await polling_task

    finally:
        logger.info("Shutting down")

        if SEEDER.is_running():
            await SEEDER.stop()

        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            pass

        try:
            await dp.stop_polling()
        except Exception:
            pass

        try:
            await user_client.stop()
        except Exception:
            pass

        try:
            await bot.session.close()
        except Exception:
            pass

        try:
            await runner.cleanup()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
