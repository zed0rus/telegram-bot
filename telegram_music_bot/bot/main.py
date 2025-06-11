import logging
import os
import json
import asyncio
import subprocess # For running ffmpeg
import shutil # For checking ffmpeg path
import yt_dlp
import time # For unique FIFO names
from typing import Dict, Any, List, Optional # For type hinting

from configparser import ConfigParser

# python-telegram-bot imports
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, Application as PTBApplication

# Telethon imports
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.phone import (
    GetGroupCallRequest, CreateGroupCallRequest, JoinGroupCallRequest,
    LeaveGroupCallRequest, DiscardGroupCallRequest, EditGroupCallParticipantRequest
)
from telethon.tl.types import (
    InputPeerChat, InputPeerChannel, InputGroupCall, GroupCall,
    InputGroupCallStream, GroupCallParticipant, DataJSON # Added DataJSON
)
from telethon.errors import (
    GroupCallNotFoundError, GroupCallForbiddenError, GroupCallInvalidError,
    UserAlreadyParticipantError, NoPasswordProvidedError
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Configuration ---
config = ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'config.ini')

if not os.path.exists(config_path):
    logger.critical("Configuration file not found. Bot cannot start.")
    TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN"; API_ID = 0; API_HASH = "YOUR_API_HASH"
else:
    config.read(config_path)
    TELEGRAM_BOT_TOKEN = config.get('Telegram', 'token', fallback='YOUR_BOT_TOKEN')
    API_ID = config.getint('Telegram', 'api_id', fallback=None)
    API_HASH = config.get('Telegram', 'api_hash', fallback=None)

FFMPEG_PATH = shutil.which('ffmpeg') or '/usr/bin/ffmpeg'
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'downloads')
FIFO_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'fifos')
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(FIFO_DIR, exist_ok=True)

# --- yt-dlp settings ---
YDL_OPTS_INFO = {'format': 'bestaudio/best', 'noplaylist': True, 'quiet': True, 'extract_flat': 'discard_in_playlist'}
YDL_OPTS_DOWNLOAD = {
    'format': 'bestaudio/best', 'noplaylist': True, 'quiet': False,
    'outtmpl': os.path.join(DOWNLOAD_DIR, '%(extractor)s-%(id)s-%(title).80s.%(ext)s'),
    'prefer_ffmpeg': True, 'ffmpeg_location': FFMPEG_PATH,
    'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'opus', 'preferredquality': '192'}],
}

# --- Type Definitions for State ---
ChatID = int
SongInfo = Dict[str, Any]
ChatState = Dict[str, Any] # Keys: 'group_call', 'status', 'ffmpeg_process', 'fifo_path', 'current_song', 'queue'

# --- Global Variables ---
ptb_application: Optional[PTBApplication] = None
telethon_client: Optional[TelegramClient] = None
active_chats: Dict[ChatID, ChatState] = {}


# --- Helper Functions ---
async def ydl_fetch_info(url: str, download: bool = False) -> Optional[SongInfo]:
    # (Same as before, ensure it returns filepath if downloaded)
    logger.info(f"Fetching info for URL: {url}, Download: {download}")
    opts = YDL_OPTS_DOWNLOAD if download else YDL_OPTS_INFO
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=download)
            filepath = None
            if download:
                if info.get('requested_downloads'):
                    filepath = info['requested_downloads'][0].get('filepath')
                    if not filepath or not os.path.exists(filepath):
                         filepath = info['requested_downloads'][0].get('_filename')
                if not filepath and info.get('filename'): filepath = info.get('filename')
                if not filepath and info.get('_filename'): filepath = info.get('_filename')
                if filepath and not os.path.exists(filepath): # Final check if path is valid
                    logger.error(f"yt-dlp claims download but path invalid: {filepath}")
                    filepath = None # Invalidate if not found

            audio_url = None; title = info.get('title', 'Unknown Title'); duration = info.get('duration', 0)
            if 'formats' in info:
                for f_format in info['formats']: # Changed variable name
                    if f_format.get('url') and f_format.get('acodec') != 'none': audio_url = f_format['url']; break
            if not audio_url and info.get('url'): audio_url = info.get('url')

            logger.info(f"Title: {title}, Duration: {duration}s, Filepath: {filepath if filepath else 'N/A'}")
            return {'title': title, 'duration': duration, 'audio_url': audio_url,
                    'webpage_url': info.get('webpage_url', url), 'extractor': info.get('extractor_key'),
                    'id': info.get('id'), 'filepath': filepath, 'query': url} # Store original query
    except Exception as e:
        logger.error(f"yt-dlp error for {url} (download={download}): {e}", exc_info=True)
        return None

async def get_telethon_input_peer(chat_id_ptb: ChatID):
    # (Same as before)
    try: return await telethon_client.get_input_entity(chat_id_ptb)
    except Exception as e: logger.error(f"Telethon peer error for {chat_id_ptb}: {e}"); return None

async def stop_current_playback(chat_id: ChatID, clear_song_data: bool = True):
    logger.info(f"Stopping playback for chat {chat_id}. Clear song data: {clear_song_data}")
    chat_data = active_chats.get(chat_id)
    if not chat_data: return

    if chat_data.get('ffmpeg_process'):
        try:
            process = chat_data['ffmpeg_process']
            if process.returncode is None: # Process is still running
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    logger.warning(f"FFmpeg (PID {process.pid}) didn't terminate, killing.")
                    process.kill()
                logger.info(f"FFmpeg process stopped for chat {chat_id}.")
        except Exception as e_ffmpeg_stop: logger.error(f"Error stopping ffmpeg for {chat_id}: {e_ffmpeg_stop}")
        chat_data['ffmpeg_process'] = None

    if chat_data.get('fifo_path') and os.path.exists(chat_data['fifo_path']):
        try: os.remove(chat_data['fifo_path']); logger.info(f"Removed FIFO: {chat_data['fifo_path']}")
        except Exception as e_fifo: logger.error(f"Error removing FIFO {chat_data['fifo_path']}: {e_fifo}")
    chat_data['fifo_path'] = None

    if clear_song_data:
        chat_data['current_song'] = None
    # Keep status as 'active' if still in call, or set to 'inactive' if also leaving call elsewhere
    if chat_data.get('status') == 'playing':
         chat_data['status'] = 'active'


async def play_next_in_queue(chat_id: ChatID, update_ptb_context: Optional[Update] = None):
    logger.info(f"Attempting to play next in queue for chat {chat_id}")
    chat_data = active_chats.get(chat_id)
    if not chat_data or not chat_data.get('group_call'):
        logger.warning(f"No active call or chat data for {chat_id} to play next.")
        if ptb_application and update_ptb_context : # Check if we can send a message
             await ptb_application.bot.send_message(chat_id, "Error: Not in a call or chat state lost.")
        return

    await stop_current_playback(chat_id) # Clean up previous song if any

    if not chat_data['queue']:
        logger.info(f"Queue for chat {chat_id} is empty.")
        chat_data['status'] = 'active' # No longer playing
        if ptb_application and update_ptb_context: # Check if we can send a message
             await ptb_application.bot.send_message(chat_id, "Queue finished.")
        return

    song_request = chat_data['queue'].pop(0) # Get next song (FIFO)
    query_or_info = song_request.get('query_or_info') # This should be the original query or prefetched info

    # If it's just a query string, fetch/download it. If it's already info (e.g. from playlist), use it.
    song_info: Optional[SongInfo]
    if isinstance(query_or_info, str): # It's a query
        if update_ptb_context: await ptb_application.bot.send_message(chat_id, f"Downloading next: {query_or_info[:100]}...")
        song_info = await ydl_fetch_info(query_or_info, download=True)
    elif isinstance(query_or_info, dict) and query_or_info.get('filepath'): # Already processed
        song_info = query_or_info
        if update_ptb_context: await ptb_application.bot.send_message(chat_id, f"Playing next from queue: {song_info['title']}")
    else: # Unrecognized item in queue
        logger.error(f"Invalid item in queue for chat {chat_id}: {query_or_info}")
        if update_ptb_context: await ptb_application.bot.send_message(chat_id, "Error: Invalid item in queue.")
        await play_next_in_queue(chat_id, update_ptb_context) # Try next one
        return


    if not song_info or not song_info.get('filepath') or not os.path.exists(song_info['filepath']):
        logger.error(f"Failed to get/download next song or file missing: {query_or_info}")
        if ptb_application and update_ptb_context: await ptb_application.bot.send_message(chat_id, f"Error processing: {str(query_or_info)[:100]}. Skipping.")
        await play_next_in_queue(chat_id, update_ptb_context) # Try next one
        return

    filepath = song_info['filepath']
    group_call = chat_data['group_call']
    fifo_path = os.path.join(FIFO_DIR, f"stream_{chat_id}_{int(time.time())}.fifo")

    try:
        if os.path.exists(fifo_path): os.remove(fifo_path)
        os.mkfifo(fifo_path)
    except OSError as e:
        logger.error(f"Failed to create FIFO {fifo_path}: {e}")
        if ptb_application and update_ptb_context: await ptb_application.bot.send_message(chat_id, "Error creating stream pipe.")
        return

    ffmpeg_cmd = [FFMPEG_PATH, '-i', filepath, '-f', 's16le', '-ar', '48000', '-ac', '2', '-loglevel', 'error', '-nostats', fifo_path]
    logger.info(f"Starting FFmpeg for next song: {' '.join(ffmpeg_cmd)}")
    try:
        ffmpeg_process = await asyncio.create_subprocess_exec(*ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e_ffmpeg:
        logger.error(f"Failed to start FFmpeg for {filepath}: {e_ffmpeg}")
        if ptb_application and update_ptb_context: await ptb_application.bot.send_message(chat_id, "Error starting audio stream.")
        if os.path.exists(fifo_path): os.remove(fifo_path)
        return

    chat_data['ffmpeg_process'] = ffmpeg_process
    chat_data['fifo_path'] = fifo_path
    chat_data['current_song'] = song_info
    chat_data['status'] = 'playing'

    # Telethon streaming logic (still experimental for bots)
    try:
        self_peer = await telethon_client.get_me(input_peer=True)
        # This is the placeholder for actual Telethon streaming call.
        # The `params` argument for EditGroupCallParticipantRequest is DataJSON.
        # For a bot to stream, it typically involves manipulating its 'speaking' state
        # and ensuring its audio input is sourced from the pipe.
        # This is highly dependent on the specific Telethon version and MTProto layer details.
        # For now, we log that we *would* start streaming.
        logger.info(f"Telethon: Would start streaming from {fifo_path} for chat {chat_id} (Bot: {self_peer.user_id}) in call {group_call.id}. (THIS IS THE EXPERIMENTAL PART)")
        # Example of what might be needed (conceptual, likely incorrect DataJSON structure):
        # params_json = json.dumps({"source": "pipe", "path": fifo_path, "speaking": True, "muted": False})
        # await telethon_client(EditGroupCallParticipantRequest(
        # call=group_call,
        # participant=self_peer,
        # params=DataJSON(data=params_json)
        # # OR: params might directly take InputGroupCallStream for some versions/setups.
        # # params=InputGroupCallStream(pipe_path=fifo_path) # This was the previous attempt
        # ))
        # The crucial part is that Telethon needs to know the bot is speaking AND where its audio comes from.
        # The previous attempt with InputGroupCallStream in params was a guess.
        # Actual implementation may need more complex setup or different requests.
        # For now, the bot SENDS a message that it's playing. The actual audio might not work.

        if ptb_application and update_ptb_context: await ptb_application.bot.send_message(chat_id, f"▶️ Now playing: {song_info['title']}")
        logger.info(f"Playback of {song_info['title']} initiated for chat {chat_id}.")

        async def _monitor_ffmpeg_task(process, chat_id_local, current_song_title, update_ctx):
            stdout, stderr = await process.communicate()
            logger.info(f"FFmpeg for '{current_song_title}' (PID: {process.pid}) in chat {chat_id_local} ended with {process.returncode}.")
            if stdout: logger.info(f"[FFMPEG STDOUT {chat_id_local}]: {stdout.decode(errors='ignore')}")
            if stderr: logger.error(f"[FFMPEG STDERR {chat_id_local}]: {stderr.decode(errors='ignore')}")

            # Check if this song is still the 'current_song' to prevent race conditions with /skip
            current_chat_data = active_chats.get(chat_id_local)
            if current_chat_data and current_chat_data.get('current_song') and current_chat_data['current_song']['title'] == current_song_title:
                await play_next_in_queue(chat_id_local, update_ctx) # Pass PTB update context
            else:
                logger.info(f"Song '{current_song_title}' in chat {chat_id_local} was likely skipped or stopped. Not playing next automatically here.")

        asyncio.create_task(_monitor_ffmpeg_task(ffmpeg_process, chat_id, song_info['title'], update_ptb_context))

    except Exception as e_stream:
        logger.error(f"Error during Telethon stream setup for {chat_id}: {e_stream}", exc_info=True)
        if ptb_application and update_ptb_context: await ptb_application.bot.send_message(chat_id, "Error starting stream with Telegram.")
        await stop_current_playback(chat_id)


async def ensure_voice_chat_joined(chat_id_ptb: ChatID, update_ptb: Update) -> bool:
    # (Slightly refactored version from previous step, ensure active_chats init)
    if chat_id_ptb in active_chats and active_chats[chat_id_ptb].get('status') in ['active', 'playing', 'paused']:
        return True

    peer = await get_telethon_input_peer(chat_id_ptb)
    if not peer: await update_ptb.message.reply_text("Chat not found."); return False

    active_chats.setdefault(chat_id_ptb, {'queue': [], 'status': 'inactive', 'ffmpeg_process': None, 'current_song': None, 'fifo_path': None, 'group_call': None})

    try:
        call_result = await telethon_client(GetGroupCallRequest(peer=peer))
        group_call: GroupCall = call_result.call
        self_peer = await telethon_client.get_me(input_peer=True)
        await telethon_client(JoinGroupCallRequest(call=group_call, join_as_peer=self_peer, muted=True))
        active_chats[chat_id_ptb]['group_call'] = group_call
        active_chats[chat_id_ptb]['status'] = 'active'
        logger.info(f"Joined existing VC in {chat_id_ptb}")
        return True
    except GroupCallNotFoundError:
        logger.info(f"No VC in {chat_id_ptb}, creating.")
        try:
            self_peer = await telethon_client.get_me(input_peer=True)
            random_id = int.from_bytes(os.urandom(4), 'big')
            updates = await telethon_client(CreateGroupCallRequest(peer=peer, random_id=random_id, title="Music Session"))
            created_call = None
            if hasattr(updates, 'updates'): # Process updates to find the call
                for u_item in updates.updates:
                    if hasattr(u_item, 'call') and isinstance(u_item.call, GroupCall):
                        created_call = u_item.call; break
            if not created_call: # Fallback: re-fetch
                refetched = await telethon_client(GetGroupCallRequest(peer=peer))
                created_call = refetched.call

            if created_call and created_call.id:
                active_chats[chat_id_ptb]['group_call'] = created_call
                active_chats[chat_id_ptb]['status'] = 'active'
                await update_ptb.message.reply_text("Created and joined VC.")
                return True
            else: await update_ptb.message.reply_text("Failed to create VC."); return False
        except Exception as e_create: logger.error(f"Err creating VC: {e_create}"); await update_ptb.message.reply_text(f"Err creating VC: {e_create}"); return False
    except UserAlreadyParticipantError: # Bot already in call
        logger.info(f"Bot already in VC for {chat_id_ptb}. Ensuring state.")
        if not active_chats[chat_id_ptb].get('group_call'): # If state lost, re-fetch
             call_re_fetch = await telethon_client(GetGroupCallRequest(peer=peer))
             active_chats[chat_id_ptb]['group_call'] = call_re_fetch.call
        active_chats[chat_id_ptb]['status'] = 'active'
        return True
    except Exception as e_join_ensure:
        logger.error(f"Error ensuring VC for {chat_id_ptb}: {e_join_ensure}", exc_info=True)
        await update_ptb.message.reply_text("Unexpected VC error.")
        return False


# --- PTB Command Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_html(f"Hi {update.effective_user.mention_html()}! /play <song/URL>, /skip, /stop, /queue.")

async def play_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    query = " ".join(context.args)
    if not query: await update.message.reply_text("Usage: /play <song name or URL>"); return

    if not await ensure_voice_chat_joined(chat_id, update):
        # ensure_voice_chat_joined already sends messages on failure
        return

    chat_data = active_chats[chat_id]
    # Add to queue
    # For simplicity, always add then try to play if nothing's playing.
    # This avoids race conditions if multiple /play commands come fast.
    chat_data['queue'].append({'type': 'query', 'query_or_info': query, 'requested_by': update.effective_user.id})
    await update.message.reply_text(f"Added to queue: {query[:100]}")

    if chat_data.get('status') != 'playing': # If not already playing something
        await play_next_in_queue(chat_id, update) # Pass PTB update for messaging
    else: # Already playing, just inform about queue position
        await update.message.reply_text(f"Position in queue: {len(chat_data['queue'])}")


async def skip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    chat_data = active_chats.get(chat_id)

    if not chat_data or chat_data.get('status') != 'playing':
        await update.message.reply_text("Not playing anything to skip.")
        return

    await update.message.reply_text("Skipping current song...")
    # stop_current_playback will be called by play_next_in_queue.
    # We need to ensure the ffmpeg monitor for the *current* song doesn't trigger play_next_in_queue again
    # after skip has already done so. Setting current_song to None helps.
    current_song_title = chat_data.get('current_song', {}).get('title', 'Unknown')
    logger.info(f"Skip requested for '{current_song_title}' in chat {chat_id}")
    chat_data['current_song'] = None # Mark as intentionally skipped

    await stop_current_playback(chat_id, clear_song_data=False) # Keep current_song as is for _monitor to check
    await play_next_in_queue(chat_id, update)


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    chat_data = active_chats.get(chat_id)
    if not chat_data or chat_data.get('status') not in ['playing', 'paused', 'active']: # Can stop even if just in call
        await update.message.reply_text("Not doing anything much to stop.")
        return

    await update.message.reply_text("⏹️ Stopping playback and clearing queue...")
    chat_data['queue'] = [] # Clear queue
    chat_data['current_song'] = None # Mark as stopped to prevent _monitor_ffmpeg from playing next
    await stop_current_playback(chat_id)
    # Note: stop_current_playback sets status to 'active'.
    # If you want /stop to also make bot leave VC, call leave_vc_command logic here.
    await update.message.reply_text("Playback stopped. Queue cleared. I'm still in the voice chat. Use /leavevc to make me leave.")

async def queue_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    chat_data = active_chats.get(chat_id)

    if not chat_data or (not chat_data.get('current_song') and not chat_data.get('queue')):
        await update.message.reply_text("Nothing playing and queue is empty.")
        return

    message = ""
    if chat_data.get('current_song'):
        message += f"▶️ Now Playing: {chat_data['current_song']['title']}\n"

    if chat_data.get('queue'):
        message += "\n📜 Queue:\n"
        for i, item in enumerate(chat_data['queue']):
            query = item.get('query_or_info')
            title = query if isinstance(query, str) else query.get('title', 'Unknown Queued Item')
            message += f"{i+1}. {str(title)[:80]}\n" # Display query or title
            if i > 10 : message += "...and more.\n"; break # Limit display length
    else:
        message += "\nQueue is empty."

    await update.message.reply_text(message)


async def leave_vc_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # (Same as before, but ensure queue is cleared and playback fully stopped)
    chat_id = update.effective_chat.id
    chat_data = active_chats.get(chat_id)
    if chat_data: # If there's any state for this chat
        chat_data['queue'] = [] # Clear queue
        chat_data['current_song'] = None # Mark as stopped
        await stop_current_playback(chat_id) # Stop playback if any

    if not telethon_client or not telethon_client.is_connected(): pass

    call_info_data = active_chats.get(chat_id) # Re-fetch after potential modification by stop_current_playback

    # If group_call info was removed by stop_current_playback if it reset the whole chat_data
    # We need to ensure we can still attempt to leave if the bot *thinks* it's in a call.
    # For robust leave, it's better to try leaving even if local state is partially lost.
    # However, LeaveGroupCallRequest needs the group_call object.

    # Let's refine: if no group_call object in state, can't issue LeaveGroupCallRequest.
    if not call_info_data or not call_info_data.get('group_call'):
        await update.message.reply_text("Not in an active voice chat, or state is inconsistent.")
        active_chats.pop(chat_id, None); return

    group_call_obj = call_info_data['group_call']
    try:
        await telethon_client(LeaveGroupCallRequest(call=group_call_obj))
        await update.message.reply_text("Left voice chat.")
    except Exception as e: logger.error(f"Err leaving VC: {e}"); await update.message.reply_text(f"Err leaving: {e}")
    finally: active_chats.pop(chat_id, None)


# --- Main Application Setup and Run ---
async def main_async():
    global ptb_application, telethon_client # Allow modification by this function

    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == 'YOUR_BOT_TOKEN': logger.critical("Bot Token missing!"); return
    if not API_ID or not API_HASH or API_HASH == 'YOUR_API_HASH': logger.critical("API ID/Hash missing!"); return
    if not FFMPEG_PATH or not os.path.exists(FFMPEG_PATH): logger.warning(f"ffmpeg not at {FFMPEG_PATH}!")

    logger.info("Initializing PTB Application...")
    ptb_application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    ptb_application.add_handler(CommandHandler("start", start_command))
    ptb_application.add_handler(CommandHandler("play", play_command))
    ptb_application.add_handler(CommandHandler("skip", skip_command))
    ptb_application.add_handler(CommandHandler("stop", stop_command))
    ptb_application.add_handler(CommandHandler("queue", queue_command))
    ptb_application.add_handler(CommandHandler("leavevc", leave_vc_command))

    logger.info("Initializing Telethon Client...")
    telethon_client = TelegramClient('music_bot_session', API_ID, API_HASH)

    try:
        logger.info("Starting Telethon client...")
        await telethon_client.start(bot_token=TELEGRAM_BOT_TOKEN)
        if not await telethon_client.is_bot(): logger.error("Telethon not bot!"); return
        logger.info("Telethon client confirmed as bot.")

        logger.info("Starting PTB application polling...")
        await ptb_application.initialize()
        await ptb_application.start()
        await ptb_application.updater.start_polling(drop_pending_updates=True)
        logger.info("Bot is running!")

        while True:
            if not telethon_client.is_connected():
                logger.warning("Telethon disconnected. Reconnecting...")
                try:
                    await telethon_client.connect()
                    if await telethon_client.is_bot(): logger.info("Telethon reconnected.")
                    else: logger.error("Telethon reconnected but no longer bot?"); break
                except Exception as e_conn: logger.error(f"Telethon reconnect failed: {e_conn}")
            await asyncio.sleep(60)
    except Exception as e: logger.error(f"Critical error in main_async: {e}", exc_info=True)
    finally:
        logger.info("Cleaning up...")
        if ptb_application and getattr(ptb_application.updater, 'running', False):
            await ptb_application.updater.stop(); await ptb_application.stop(); await ptb_application.shutdown()
        for chat_id_cleanup in list(active_chats.keys()): await stop_current_playback(chat_id_cleanup)
        if telethon_client and telethon_client.is_connected(): await telethon_client.disconnect()
        logger.info("Bot stopped.")

if __name__ == '__main__':
    try: asyncio.run(main_async())
    except Exception as e: logger.critical(f"Failed to run bot: {e}", exc_info=True)
