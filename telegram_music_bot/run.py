import sys
import os
import asyncio
import logging

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from bot.main import main_async # This will point to the new main.py to be created

if __name__ == '__main__':
    logger.info("Starting bot from run.py using asyncio.run(main_async())...")
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt in run.py).")
    except Exception as e:
        logger.critical(f"Unhandled exception in run.py: {e}", exc_info=True)
EOF
