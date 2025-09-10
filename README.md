# Token Tony - Telegram Bot

Token Tony is a Telegram bot designed to provide real-time price and market data for Solana tokens using the DexScreener API.

## Features

- `/tony [token_address]`: Get a detailed report about a token, including price, market cap, liquidity, volume, and more.
- `/t [token_address]`: A quick command to get just the current price of a token.
- `/start`: Display a welcome message with instructions.

## Setup and Installation

### Prerequisites
- Python 3.8 or higher
- A Telegram Bot Token (get one from [BotFather](https://t.me/BotFather))

### Steps

1.  **Clone the Repository**
    ```bash
    git clone https://github.com/token-tony/Token_Tony_V11.git
    cd Token_Tony_V11
    ```

2.  **Install Dependencies**
    Install all the required Python packages using pip.
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables**
    Create a file named `.env` in the root of the project folder. This file will securely store your Telegram bot token. Add the following line to it, replacing `YOUR_TELEGRAM_TOKEN_HERE` with your actual token from BotFather.
    ```
    TELEGRAM_BOT_TOKEN=YOUR_TELEGRAM_TOKEN_HERE
    ```

4.  **Run the Bot**
    Start the bot with the following command:
    ```bash
    python main.py
    ```

The bot should now be running. You can interact with it on Telegram by finding it and sending a command.

---

**Final Instructions:**  
- Update Files: Replace the content of the files as specified above.  
- Install Libraries: Open your terminal in the project folder and run:  
  `pip install -r requirements.txt`  
- Set Token: Create a .env file and add your TELEGRAM_BOT_TOKEN to it.  
- Run: Start your new Telegram bot:  
  `python main.py`  
  
You are now fully converted to a faster, more reliable Telegram bot.
