import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import Message

load_dotenv()

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

SESSIONS_DIR = os.getenv("SESSIONS_DIR", "sessions").strip() or "sessions"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

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
logger = logging.getLogger("inline-seeder-userbot")


def clean_value(value: str) -> str:
    return " ".join((value or "").split()).strip()


def normalize_username(value: str) -> str:
    return clean_value(value).lstrip("@").lower()


def parse_chat_ref(value: str):
    value = clean_value(value)
    if value.lstrip("-").isdigit():
        return int(value)
    return value


DEFAULT_TARGET_CHAT = parse_chat_ref(DEFAULT_TARGET_CHAT_RAW)
RESOLVED_TARGET_CHAT: str | int = DEFAULT_TARGET_CHAT


def target_chat_display() -> str:
    return str(RESOLVED_TARGET_CHAT)


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


def is_target_chat_message(message: Message) -> bool:
    if not message.chat:
        return False

    if isinstance(RESOLVED_TARGET_CHAT, int):
        return message.chat.id == RESOLVED_TARGET_CHAT

    target_username = normalize_username(str(RESOLVED_TARGET_CHAT))
    chat_username = normalize_username(getattr(message.chat, "username", "") or "")
    return bool(chat_username and chat_username == target_username)


def is_owner_or_self_message(message: Message) -> bool:
    if not message.from_user:
        return False

    if message.from_user.id in OWNER_IDS:
        return True

    if getattr(message, "outgoing", False):
        return True

    return False


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
                "target_chat": str(RESOLVED_TARGET_CHAT),
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
            target_chat=RESOLVED_TARGET_CHAT,
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
                    chat_id=RESOLVED_TARGET_CHAT,
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

            except ValueError as e:
                if "Peer id invalid" in str(e):
                    msg = (
                        f"Peer id invalid for target chat {RESOLVED_TARGET_CHAT}. "
                        "User account ကို target group ထဲ join ထားပြီး "
                        "group ကိုတစ်ခါဖွင့်၊ message တစ်ခါပို့ပြီး app ကိုပြန် run ပါ။"
                    )
                    self.state.last_error = msg
                    raise RuntimeError(msg) from e
                raise

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

    async def _notify_target_chat_error(self, text: str) -> None:
        try:
            await self.client.send_message(
                chat_id=RESOLVED_TARGET_CHAT,
                text=text,
            )
        except Exception:
            logger.exception("Failed to send error notification to target chat")

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
                        RESOLVED_TARGET_CHAT,
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
                "⚠️ Inline Seeder Stopped\n\n"
                f"Source bot: {source_bot}\n"
                f"Target chat: {RESOLVED_TARGET_CHAT}\n"
                f"Sent count: {self.state.sent_count}\n"
                f"Current offset: {self.state.current_offset or '-'}\n"
                f"Current index: {self.state.current_index}\n"
                f"Last error: {self.state.last_error or 'unknown'}"
            )
            await self._notify_target_chat_error(error_text)

        finally:
            self.state.running = False
            if manual_stop:
                logger.info("Seeder worker stopped manually")
            else:
                logger.info("Seeder worker stopped")


Path(SESSIONS_DIR).mkdir(parents=True, exist_ok=True)

app = Client(
    name="inline_seeder_userbot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workdir=SESSIONS_DIR,
)

SEEDER = InlineSeeder(app)


def parse_delay_from_text(text: str, default_delay: int) -> int:
    parts = clean_value(text).split()
    if len(parts) < 2:
        return default_delay
    if not parts[1].isdigit():
        raise ValueError("Delay must be a number")
    return int(parts[1])


def command_name(text: str) -> str:
    return clean_value(text).split()[0].lower() if clean_value(text) else ""


async def warmup_client_peers(client: Client) -> None:
    global RESOLVED_TARGET_CHAT

    try:
        async for dialog in client.get_dialogs():
            _ = dialog.chat.id
        logger.info("Dialogs warmup completed")
    except Exception as e:
        logger.warning("Dialogs warmup failed: %s", e)

    try:
        chat = await client.get_chat(DEFAULT_TARGET_CHAT)
        RESOLVED_TARGET_CHAT = chat.id
        title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or ""
        logger.info("Target chat resolved: %s (%s)", title, chat.id)
    except Exception as e:
        logger.error("Failed to resolve DEFAULT_TARGET_CHAT=%r : %s", DEFAULT_TARGET_CHAT, e)
        raise RuntimeError(
            "DEFAULT_TARGET_CHAT ကို user session က resolve မလုပ်နိုင်သေးပါ။ "
            "user account ကို target group ထဲ join ထားပြီး group ကိုတစ်ခါဖွင့်၊ "
            "message တစ်ခါပို့ပြီးမှ app ကိုပြန် run ပါ။"
        ) from e


@app.on_message(filters.text)
async def command_handler(client: Client, message: Message):
    text = clean_value(message.text or "")
    chat_id = getattr(message.chat, "id", None)
    user_id = getattr(message.from_user, "id", None) if message.from_user else None

    if is_target_chat_message(message):
        logger.info(
            "TARGET MSG | chat_id=%s | user_id=%s | text=%r",
            chat_id,
            user_id,
            text,
        )

    if not is_target_chat_message(message):
        return

    if user_id not in OWNER_IDS:
        logger.info("IGNORED NON-OWNER | user_id=%s | text=%r", user_id, text)
        return

    cmd = command_name(text)

    if cmd not in {
        "/start",
        "/status",
        "/startcatcherbot",
        "/startseizerbot",
        "/stopinlinebot",
        "/resetinlineprogress",
    }:
        logger.info("IGNORED UNKNOWN CMD | user_id=%s | text=%r", user_id, text)
        return

    try:
        if cmd == "/start":
            await message.reply(
                "Inline Seeder ready.\n\n"
                f"Mode: userbot only\n"
                f"Target chat: {target_chat_display()}\n"
                f"Catcher: {CATCHER_INLINE_BOT}\n"
                f"Seizer: {SEIZER_INLINE_BOT}\n"
                f"Default delay: {DEFAULT_SEND_DELAY}s\n"
                f"Max retries: {MAX_RETRIES}\n"
                f"Retry base delay: {RETRY_BASE_DELAY}s\n"
                f"State file: {STATE_FILE}\n"
                f"Sessions dir: {SESSIONS_DIR}\n\n"
                "Commands:\n"
                "/startcatcherbot\n"
                "/startcatcherbot 5\n"
                "/startseizerbot\n"
                "/startseizerbot 10\n"
                "/stopinlinebot\n"
                "/resetinlineprogress\n"
                "/status"
            )
            return

        if cmd == "/status":
            me = await client.get_me()
            state = SEEDER.state
            await message.reply(
                f"Running: {'YES' if SEEDER.is_running() else 'NO'}\n"
                f"User session: {me.first_name or ''} ({me.id})\n"
                f"Source bot: {state.source_bot or '-'}\n"
                f"Target chat: {state.target_chat}\n"
                f"Delay: {state.delay_seconds}s\n"
                f"Sent count: {state.sent_count}\n"
                f"Current offset: {state.current_offset or '-'}\n"
                f"Current index: {state.current_index}\n"
                f"Last error: {state.last_error or '-'}"
            )
            return

        if cmd == "/stopinlinebot":
            stopped = await SEEDER.stop()
            if not stopped:
                await message.reply("Seeder is not running.")
                return
            await message.reply(f"Stopped.\nSent count: {SEEDER.state.sent_count}")
            return

        if cmd == "/resetinlineprogress":
            if SEEDER.is_running():
                await message.reply("Seeder running ဖြစ်နေပါတယ်။ အရင် /stopinlinebot လုပ်ပါ။")
                return
            clear_progress_state()
            await message.reply("Inline progress state cleared.")
            return

        if cmd == "/startcatcherbot":
            delay_seconds = parse_delay_from_text(text, DEFAULT_SEND_DELAY)
            await SEEDER.start(CATCHER_INLINE_BOT, delay_seconds)
            await message.reply(
                "Started.\n"
                f"Source bot: {CATCHER_INLINE_BOT}\n"
                f"Target chat: {target_chat_display()}\n"
                f"Delay: {delay_seconds}s\n"
                f"Resume from offset: {SEEDER.state.current_offset or '-'}\n"
                f"Resume from index: {SEEDER.state.current_index}"
            )
            return

        if cmd == "/startseizerbot":
            delay_seconds = parse_delay_from_text(text, DEFAULT_SEND_DELAY)
            await SEEDER.start(SEIZER_INLINE_BOT, delay_seconds)
            await message.reply(
                "Started.\n"
                f"Source bot: {SEIZER_INLINE_BOT}\n"
                f"Target chat: {target_chat_display()}\n"
                f"Delay: {delay_seconds}s\n"
                f"Resume from offset: {SEEDER.state.current_offset or '-'}\n"
                f"Resume from index: {SEEDER.state.current_index}"
            )
            return

    except Exception as e:
        logger.exception("COMMAND ERROR")
        await message.reply(f"Error: {e}")


async def main():
    await app.start()
    await warmup_client_peers(app)

    me = await app.get_me()
    logger.info("User session started as %s (%s)", me.first_name, me.id)
    logger.info("Target chat: %r", RESOLVED_TARGET_CHAT)
    logger.info("Owner IDs: %s", sorted(OWNER_IDS))
    
    try:
    await app.send_message(DEFAULT_TARGET_CHAT, "AddHelper online ✅")
    logger.info("Startup test message sent to target chat")
    except Exception as e:
    logger.error("Startup test message failed: %s", e)

    stop_event = asyncio.Event()

    def _stop():
        logger.info("Stop signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    await stop_event.wait()

    if SEEDER.is_running():
        await SEEDER.stop()

    await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
