import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message
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

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BASE_DELAY = int(os.getenv("RETRY_BASE_DELAY", "3"))

STATE_FILE = os.getenv("STATE_FILE", "seeder_state.json").strip()
CLEAR_STATE_ON_FINISH = os.getenv("CLEAR_STATE_ON_FINISH", "true").lower() == "true"

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


def _state_path() -> Path:
    return Path(STATE_FILE)


def load_progress_state() -> dict:
    path = _state_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read state file")
        return {}


def save_progress_state(data: dict) -> None:
    path = _state_path()
    try:
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        logger.exception("Failed to write state file")


def clear_progress_state() -> None:
    path = _state_path()
    try:
        if path.exists():
            path.unlink()
    except Exception:
        logger.exception("Failed to clear state file")


async def notify_target_chat_error(text: str) -> None:
    global bot

    if bot is None:
        return

    try:
        await bot.send_message(
            chat_id=DEFAULT_TARGET_CHAT,
            text=text,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        logger.exception("Failed to send error notification to target chat")


@dataclass
class RunnerState:
    running: bool = False
    sent_count: int = 0
    delay_seconds: int = DEFAULT_SEND_DELAY
    target_chat: str | int = DEFAULT_TARGET_CHAT
    current_offset: str = ""
    current_index: int = 0
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

    def _load_resume_state_for_bot(self, source_bot: str) -> tuple[str, int, int]:
        data = load_progress_state()
        saved_bot = str(data.get("source_bot") or "")
        if not data or saved_bot != source_bot:
            return "", 0, 0

        offset = str(data.get("current_offset") or "")
        index = int(data.get("current_index") or 0)
        sent_count = int(data.get("sent_count") or 0)
        return offset, index, sent_count

    def _save_resume_state(self, source_bot: str, offset: str, index: int) -> None:
        save_progress_state(
            {
                "source_bot": source_bot,
                "target_chat": str(DEFAULT_TARGET_CHAT),
                "current_offset": offset,
                "current_index": index,
                "sent_count": self.state.sent_count,
                "delay_seconds": self.state.delay_seconds,
            }
        )

    async def start(self, source_bot: str, delay_seconds: int):
        if self.is_running():
            raise RuntimeError("Seeder is already running")

        delay_seconds = max(1, min(delay_seconds, MAX_SEND_DELAY))
        resume_offset, resume_index, resume_sent_count = self._load_resume_state_for_bot(source_bot)

        self.stop_event = asyncio.Event()
        self.state = RunnerState(
            running=True,
            sent_count=resume_sent_count,
            delay_seconds=delay_seconds,
            target_chat=DEFAULT_TARGET_CHAT,
            current_offset=resume_offset,
            current_index=resume_index,
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

    async def _sleep_with_stop(self, seconds: float):
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    def _backoff_delay(self, attempt: int) -> int:
        return RETRY_BASE_DELAY * (2 ** max(0, attempt - 1))

    async def _retry_get_inline_results(self, source_bot: str, offset: str):
        last_exc: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_event.is_set():
                raise asyncio.CancelledError()

            try:
                return await self.client.get_inline_bot_results(
                    bot=source_bot,
                    query="",
                    offset=offset,
                )

            except FloodWait as e:
                wait_for = int(getattr(e, "value", getattr(e, "x", 5)))
                self.state.last_error = f"FloodWait(get_results): {wait_for}s"
                logger.warning(
                    "FloodWait on get_inline_bot_results from %s | attempt %s/%s | waiting %ss",
                    source_bot,
                    attempt,
                    MAX_RETRIES,
                    wait_for,
                )
                await self._sleep_with_stop(wait_for + 1)
                last_exc = e

            except (RPCError, OSError, TimeoutError) as e:
                delay = self._backoff_delay(attempt)
                self.state.last_error = f"get_results retry error: {e}"
                logger.warning(
                    "get_inline_bot_results failed from %s | attempt %s/%s | retry in %ss | error=%s",
                    source_bot,
                    attempt,
                    MAX_RETRIES,
                    delay,
                    e,
                )
                last_exc = e
                if attempt < MAX_RETRIES:
                    await self._sleep_with_stop(delay)
                else:
                    break

            except Exception as e:
                self.state.last_error = f"get_results fatal: {e}"
                logger.exception("Unexpected get_inline_bot_results error")
                raise

        raise RuntimeError(f"get_inline_bot_results failed after {MAX_RETRIES} retries: {last_exc}")

    async def _retry_send_inline_result(self, source_bot: str, results, result_id: str):
        last_exc: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):
            if self.stop_event.is_set():
                raise asyncio.CancelledError()

            try:
                await self.client.send_inline_bot_result(
                    chat_id=DEFAULT_TARGET_CHAT,
                    query_id=results.query_id,
                    result_id=result_id,
                )
                return

            except FloodWait as e:
                wait_for = int(getattr(e, "value", getattr(e, "x", 5)))
                self.state.last_error = f"FloodWait(send_result): {wait_for}s"
                logger.warning(
                    "FloodWait on send_inline_bot_result from %s | attempt %s/%s | waiting %ss",
                    source_bot,
                    attempt,
                    MAX_RETRIES,
                    wait_for,
                )
                await self._sleep_with_stop(wait_for + 1)
                last_exc = e

            except (RPCError, OSError, TimeoutError) as e:
                delay = self._backoff_delay(attempt)
                self.state.last_error = f"send_result retry error: {e}"
                logger.warning(
                    "send_inline_bot_result failed from %s | attempt %s/%s | retry in %ss | error=%s",
                    source_bot,
                    attempt,
                    MAX_RETRIES,
                    delay,
                    e,
                )
                last_exc = e
                if attempt < MAX_RETRIES:
                    await self._sleep_with_stop(delay)
                else:
                    break

            except Exception as e:
                self.state.last_error = f"send_result fatal: {e}"
                logger.exception("Unexpected send_inline_bot_result error")
                raise

        raise RuntimeError(f"send_inline_bot_result failed after {MAX_RETRIES} retries: {last_exc}")

    async def _worker(self, source_bot: str):
        offset = self.state.current_offset or ""
        start_index = self.state.current_index or 0
        manual_stop = False

        try:
            while not self.stop_event.is_set():
                results = await self._retry_get_inline_results(source_bot, offset)

                if not results.results:
                    logger.info("No more inline results from %s", source_bot)
                    if CLEAR_STATE_ON_FINISH:
                        clear_progress_state()
                    break

                logger.info(
                    "Fetched %s results from %s | offset=%r | next_offset=%r | start_index=%s",
                    len(results.results),
                    source_bot,
                    offset,
                    results.next_offset,
                    start_index,
                )

                safe_start_index = max(0, min(start_index, len(results.results)))

                for idx in range(safe_start_index, len(results.results)):
                    if self.stop_event.is_set():
                        manual_stop = True
                        break

                    result = results.results[idx]
                    await self._retry_send_inline_result(source_bot, results, result.id)

                    self.state.sent_count += 1
                    self.state.current_offset = offset
                    self.state.current_index = idx + 1
                    self._save_resume_state(source_bot, offset, idx + 1)

                    logger.info(
                        "Sent #%s result_id=%s from %s to %r | page_offset=%r | next_index=%s",
                        self.state.sent_count,
                        result.id,
                        source_bot,
                        DEFAULT_TARGET_CHAT,
                        offset,
                        idx + 1,
                    )

                    await self._sleep_with_stop(self.state.delay_seconds)

                    if self.stop_event.is_set():
                        manual_stop = True
                        break

                if self.stop_event.is_set():
                    manual_stop = True
                    break

                offset = results.next_offset or ""
                start_index = 0
                self.state.current_offset = offset
                self.state.current_index = 0
                self._save_resume_state(source_bot, offset, 0)

                if not offset:
                    logger.info("Reached final page for %s", source_bot)
                    if CLEAR_STATE_ON_FINISH:
                        clear_progress_state()
                    break

        except asyncio.CancelledError:
            manual_stop = True
            self.state.last_error = "cancelled"
            logger.info("Seeder worker cancelled")
            raise

        except Exception as e:
            self.state.last_error = str(e)
            logger.exception("Seeder worker failed")

            error_text = (
                "⚠️ <b>Inline Seeder Stopped</b>\n\n"
                f"Source bot: <code>{html_escape(source_bot)}</code>\n"
                f"Target chat: <code>{html_escape(str(DEFAULT_TARGET_CHAT))}</code>\n"
                f"Sent count: <code>{self.state.sent_count}</code>\n"
                f"Current offset: <code>{html_escape(self.state.current_offset or '-')}</code>\n"
                f"Current index: <code>{self.state.current_index}</code>\n"
                f"Last error: <code>{html_escape(self.state.last_error or 'unknown')}</code>"
            )
            await notify_target_chat_error(error_text)

        finally:
            self.state.running = False
            if manual_stop:
                logger.info("Seeder worker stopped manually")
            else:
                logger.info("Seeder worker stopped")


SEEDER: Optional[InlineSeeder] = None


@router.message(Command("start"))
async def start_handler(message: Message):
    if not await require_owner_in_target_chat(message):
        return

    await message.reply(
        "Inline Seeder ready.\n\n"
        f"Mode: <b>polling</b>\n"
        f"Target chat: <code>{html_escape(str(DEFAULT_TARGET_CHAT))}</code>\n"
        f"Catcher: <code>{html_escape(CATCHER_INLINE_BOT)}</code>\n"
        f"Seizer: <code>{html_escape(SEIZER_INLINE_BOT)}</code>\n"
        f"Default delay: <code>{DEFAULT_SEND_DELAY}</code>s\n"
        f"Max retries: <code>{MAX_RETRIES}</code>\n"
        f"Retry base delay: <code>{RETRY_BASE_DELAY}</code>s\n"
        f"State file: <code>{html_escape(STATE_FILE)}</code>\n\n"
        "Commands:\n"
        "/startcatcherbot\n"
        "/startcatcherbot 5\n"
        "/startseizerbot\n"
        "/startseizerbot 10\n"
        "/stopinlinebot\n"
        "/resetinlineprogress\n"
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
        f"Current index: <code>{state.current_index}</code>\n"
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
            f"Delay: <code>{delay_seconds}</code>s\n"
            f"Max retries: <code>{MAX_RETRIES}</code>\n"
            f"Resume from offset: <code>{html_escape(SEEDER.state.current_offset or '-')}</code>\n"
            f"Resume from index: <code>{SEEDER.state.current_index}</code>",
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


@router.message(Command("resetinlineprogress"))
async def resetinlineprogress_handler(message: Message):
    if not await require_owner_in_target_chat(message):
        return

    if SEEDER.is_running():
        await message.reply("Seeder running ဖြစ်နေပါတယ်။ အရင် /stopinlinebot လုပ်ပါ။")
        return

    clear_progress_state()
    await message.reply("Inline progress state cleared.")


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
    logger.info("Running in polling mode")
    logger.info("Target chat: %r", DEFAULT_TARGET_CHAT)

    stop_event = asyncio.Event()

    def _stop(*_args):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    polling_task: Optional[asyncio.Task] = None

    try:
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
            await dp.stop_polling()
        except Exception:
            pass

        if polling_task:
            polling_task.cancel()

        try:
            await user_client.stop()
        except Exception:
            pass

        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
