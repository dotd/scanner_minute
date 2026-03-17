from ScannerMinute.src.telegram_utils import send_message


def tst_send_telegram():
    send_message(
        chat_id="7900201753",
        message="Hello from ScannerMinute!",
        token="8765274293:AAGOw57j6YlMjLBs6KtWeYzyMr308Nohek8",
    )


if __name__ == "__main__":
    tst_send_telegram()
