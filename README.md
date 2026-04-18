# AddHelper

Polling-only inline seeder service for Telegram.

ဒီ service က **owner account session** ကိုသုံးပြီး  
`@Character_Catcher_Bot` နဲ့ `@Character_Seizer_Bot` inline results တွေကို  
**တစ်ပုံချင်းစီ** `DEFAULT_TARGET_CHAT` ထဲကို ပို့ပေးတဲ့ helper bot ဖြစ်ပါတယ်။

## Main Features

- Polling only
- Owner only
- Commands work **only inside `DEFAULT_TARGET_CHAT`**
- Supports:
  - `/startcatcherbot`
  - `/startseizerbot`
  - `/stopinlinebot`
  - `/resetinlineprogress`
  - `/status`
- Retry-safe inline sending
- FloodWait handling
- Resume progress after restart
- Persistent progress state with JSON file
- Works well with PM2 on VPS

---

## How It Works

This service uses **2 Telegram connections**:

### 1. Bot Token
Aiogram control bot for commands.

### 2. Session String
Pyrogram user session for:
- getting inline results
- sending inline result messages to target chat

Because Telegram Bot API cannot click inline results like a normal user,  
this project uses a **user session** to send inline results one by one.

---

## Supported Inline Bots

- `@Character_Catcher_Bot`
- `@Character_Seizer_Bot`

Commands:

- `/startcatcherbot`
- `/startseizerbot`

Both commands send inline media one-by-one into the target chat.

---

## Requirements

- Python 3.10+
- Telegram bot token
- Telegram API ID
- Telegram API HASH
- Telegram user session string
- VPS or any always-on server

---

## Installation

```bash
git clone https://github.com/OfficialBika/AddHelper.git
cd AddHelper
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
