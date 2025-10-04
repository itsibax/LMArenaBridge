# api_server.py
# Next-generation LMArena Bridge backend service

import asyncio
import json
import logging
import os
import sys
import subprocess
import time
import uuid
import re
import threading
import random
import mimetypes
from datetime import datetime
from contextlib import asynccontextmanager

import uvicorn
import requests
from packaging.version import parse as parse_version
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, Response

# --- Internal module imports ---
from modules.file_uploader import upload_to_file_bed


# --- Basic configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Global state and configuration ---
CONFIG = {} # Stores configuration loaded from config.jsonc
# browser_ws stores the WebSocket connection for a single Tampermonkey script.
# Note: this architecture assumes that only one browser tab is active.
# To support multiple concurrent tabs, extend this to a dict that tracks each connection.
browser_ws: WebSocket | None = None
# response_channels stores the response queue for every API request.
# The key is request_id and the value is an asyncio.Queue.
response_channels: dict[str, asyncio.Queue] = {}
last_activity_time = None # Tracks the timestamp of the most recent activity
idle_monitor_thread = None # Idle monitoring thread
main_event_loop = None # Main event loop
# Tracks whether we are refreshing because of a human verification challenge
IS_REFRESHING_FOR_VERIFICATION = False


# --- Model mapping ---
# MODEL_NAME_TO_ID_MAP now stores richer objects: { "model_name": {"id": "...", "type": "..."} }
MODEL_NAME_TO_ID_MAP = {}
MODEL_ENDPOINT_MAP = {} # Stores the mapping from models to session/message IDs
DEFAULT_MODEL_ID = None # Default model id: None

def load_model_endpoint_map():
    """Load the model-to-endpoint mapping from model_endpoint_map.json."""
    global MODEL_ENDPOINT_MAP
    try:
        with open('model_endpoint_map.json', 'r', encoding='utf-8') as f:
            content = f.read()
            # Allow the file to be empty
            if not content.strip():
                MODEL_ENDPOINT_MAP = {}
            else:
                MODEL_ENDPOINT_MAP = json.loads(content)
        logger.info(f"Loaded {len(MODEL_ENDPOINT_MAP)} model endpoint mappings from 'model_endpoint_map.json'.")
    except FileNotFoundError:
        logger.warning("'model_endpoint_map.json' not found. Using an empty mapping.")
        MODEL_ENDPOINT_MAP = {}
    except json.JSONDecodeError as e:
        logger.error(f"Failed to load or parse 'model_endpoint_map.json': {e}. Falling back to an empty mapping.")
        MODEL_ENDPOINT_MAP = {}

def _parse_jsonc(jsonc_string: str) -> dict:
    """Robustly parse a JSONC string by stripping comments."""
    lines = jsonc_string.splitlines()
    no_comments_lines = []
    in_block_comment = False
    for line in lines:
        stripped_line = line.strip()
        if in_block_comment:
            if '*/' in stripped_line:
                in_block_comment = False
                line = stripped_line.split('*/', 1)[1]
            else:
                continue
        
        if '/*' in line and not in_block_comment:
            before_comment, _, after_comment = line.partition('/*')
            if '*/' in after_comment:
                _, _, after_block = after_comment.partition('*/')
                line = before_comment + after_block
            else:
                line = before_comment
                in_block_comment = True

        if line.strip().startswith('//'):
            continue
        
        no_comments_lines.append(line)

    return json.loads("\n".join(no_comments_lines))

def load_config():
    """Load configuration from config.jsonc while honoring JSONC comments."""
    global CONFIG
    try:
        with open('config.jsonc', 'r', encoding='utf-8') as f:
            content = f.read()
        CONFIG = _parse_jsonc(content)
        logger.info("Loaded configuration from 'config.jsonc'.")
        # Log the key feature toggles
        logger.info(f"  - Tavern Mode: {'✅ Enabled' if CONFIG.get('tavern_mode_enabled') else '❌ Disabled'}")
        logger.info(f"  - Bypass Mode: {'✅ Enabled' if CONFIG.get('bypass_enabled') else '❌ Disabled'}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load or parse 'config.jsonc': {e}. Falling back to defaults.")
        CONFIG = {}

def load_model_map():
    """Load the model mapping from models.json, supporting the 'id:type' format."""
    global MODEL_NAME_TO_ID_MAP
    try:
        with open('models.json', 'r', encoding='utf-8') as f:
            raw_map = json.load(f)
            
        processed_map = {}
        for name, value in raw_map.items():
            if isinstance(value, str) and ':' in value:
                parts = value.split(':', 1)
                model_id = parts[0] if parts[0].lower() != 'null' else None
                model_type = parts[1]
                processed_map[name] = {"id": model_id, "type": model_type}
            else:
                # Fallback for legacy format without an explicit type
                processed_map[name] = {"id": value, "type": "text"}

        MODEL_NAME_TO_ID_MAP = processed_map
        logger.info(f"Loaded and parsed {len(MODEL_NAME_TO_ID_MAP)} models from 'models.json'.")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load 'models.json': {e}. Using an empty model list.")
        MODEL_NAME_TO_ID_MAP = {}

# --- Announcement handling ---
def check_and_display_announcement():
    """Check for a one-time announcement and print it to the logs."""
    announcement_file = "announcement-lmarena.json"
    if os.path.exists(announcement_file):
        try:
            logger.info("="*60)
            logger.info("📢 Update announcement detected:")
            with open(announcement_file, 'r', encoding='utf-8') as f:
                announcement = json.load(f)
                title = announcement.get("title", "Announcement")
                content = announcement.get("content", [])
                
                logger.info(f"   --- {title} ---")
                for line in content:
                    logger.info(f"   {line}")
                logger.info("="*60)

        except json.JSONDecodeError:
            logger.error(
                f"Unable to parse announcement file '{announcement_file}'. The file might not contain valid JSON."
            )
        except Exception as e:
            logger.error(f"Failed to read announcement file: {e}")
        finally:
            try:
                os.remove(announcement_file)
                logger.info(f"Announcement file '{announcement_file}' has been removed.")
            except OSError as e:
                logger.error(f"Failed to delete announcement file '{announcement_file}': {e}")

# --- Update checks ---
GITHUB_REPO = "Lianues/LMArenaBridge"

def download_and_extract_update(version):
    """Download the latest codebase and extract it into a temporary directory."""
    update_dir = "update_temp"
    if not os.path.exists(update_dir):
        os.makedirs(update_dir)

    try:
        zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/main.zip"
        logger.info(f"Downloading new version from {zip_url}...")
        response = requests.get(zip_url, timeout=60)
        response.raise_for_status()

        # Requires the zipfile and io modules
        import zipfile
        import io
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(update_dir)
        
        logger.info(f"New version downloaded and extracted to '{update_dir}'.")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download update: {e}")
    except zipfile.BadZipFile:
        logger.error("Downloaded file is not a valid ZIP archive.")
    except Exception as e:
        logger.error(f"Unexpected error while extracting update: {e}")
    
    return False

def check_for_updates():
    """Check GitHub for a newer release."""
    if not CONFIG.get("enable_auto_update", True):
        logger.info("Auto-update disabled; skipping check.")
        return

    current_version = CONFIG.get("version", "0.0.0")
    logger.info(f"Current version: {current_version}. Checking GitHub for updates...")

    try:
        config_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/main/config.jsonc"
        response = requests.get(config_url, timeout=10)
        response.raise_for_status()

        jsonc_content = response.text
        remote_config = _parse_jsonc(jsonc_content)
        
        remote_version_str = remote_config.get("version")
        if not remote_version_str:
            logger.warning("No version number found in remote config; skipping update check.")
            return

        if parse_version(remote_version_str) > parse_version(current_version):
            logger.info("="*60)
            logger.info("🎉 New version available! 🎉")
            logger.info(f"  - Current version: {current_version}")
            logger.info(f"  - Latest version: {remote_version_str}")
            if download_and_extract_update(remote_version_str):
                logger.info("Preparing to apply update. Server will exit in 5 seconds to launch the updater.")
                time.sleep(5)
                update_script_path = os.path.join("modules", "update_script.py")
                # Launch the updater in a detached subprocess
                subprocess.Popen([sys.executable, update_script_path])
                # Exit the current server process gracefully
                os._exit(0)
            else:
                logger.error(f"Automatic update failed. Please download manually from https://github.com/{GITHUB_REPO}/releases/latest.")
            logger.info("="*60)
        else:
            logger.info("Already running the latest version.")

    except requests.RequestException as e:
        logger.error(f"Update check failed: {e}")
    except json.JSONDecodeError:
        logger.error("Failed to parse the remote configuration file.")
    except Exception as e:
        logger.error(f"Unexpected error while checking for updates: {e}")

# --- Model update utilities ---
def extract_models_from_html(html_content):
    """
    Extract complete model JSON objects from the HTML by matching braces.
    """
    models = []
    model_names = set()
    
    # Find every potential start index for a model JSON object
    for start_match in re.finditer(r'\{\\"id\\":\\"[a-f0-9-]+\\"', html_content):
        start_index = start_match.start()
        
        # Walk forward from the start index while balancing braces
        open_braces = 0
        end_index = -1
        
        # Optimization: set a sane upper bound to avoid infinite loops
        search_limit = start_index + 10000 # Assume a single model definition fits within 10k characters
        
        for i in range(start_index, min(len(html_content), search_limit)):
            if html_content[i] == '{':
                open_braces += 1
            elif html_content[i] == '}':
                open_braces -= 1
                if open_braces == 0:
                    end_index = i + 1
                    break
        
        if end_index != -1:
            # Extract the full escaped JSON string
            json_string_escaped = html_content[start_index:end_index]
            
            # Unescape and decode it
            json_string = json_string_escaped.replace('\\"', '"').replace('\\\\', '\\')
            
            try:
                model_data = json.loads(json_string)
                model_name = model_data.get('publicName')
                
                # Deduplicate by publicName
                if model_name and model_name not in model_names:
                    models.append(model_data)
                    model_names.add(model_name)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse extracted JSON object: {e} - snippet: {json_string[:150]}...")
                continue

    if models:
        logger.info(f"Extracted and parsed {len(models)} distinct models.")
        return models
    else:
        logger.error("No complete model JSON objects were found in the HTML response.")
        return None

def save_available_models(new_models_list, models_path="available_models.json"):
    """
    Save the extracted model objects to the specified JSON file.
    """
    logger.info(f"Detected {len(new_models_list)} models; updating '{models_path}'...")
    
    try:
        with open(models_path, 'w', encoding='utf-8') as f:
            # Write the list of full model objects directly to disk
            json.dump(new_models_list, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ '{models_path}' updated successfully with {len(new_models_list)} models.")
    except IOError as e:
        logger.error(f"❌ Failed to write '{models_path}': {e}")

# --- Automatic restart logic ---
def restart_server():
    """Gracefully notify the client to refresh and then restart the server."""
    logger.warning("=" * 60)
    logger.warning("Server has been idle for too long; preparing to restart...")
    logger.warning("=" * 60)

    # 1. Notify the browser asynchronously to refresh
    async def notify_browser_refresh():
        if browser_ws:
            try:
                # Prefer sending 'reconnect' so the front-end knows a restart is expected
                await browser_ws.send_text(json.dumps({"command": "reconnect"}, ensure_ascii=False))
                logger.info("Sent 'reconnect' instruction to the browser.")
            except Exception as e:
                logger.error(f"Failed to send 'reconnect' instruction: {e}")

    # Run the async notification from the main event loop
    # Use asyncio.run_coroutine_threadsafe for thread safety
    if browser_ws and browser_ws.client_state.name == 'CONNECTED' and main_event_loop:
        asyncio.run_coroutine_threadsafe(notify_browser_refresh(), main_event_loop)

    # 2. Delay a few seconds to ensure the message is delivered
    time.sleep(3)

    # 3. Perform the restart
    logger.info("Restarting server now...")
    os.execv(sys.executable, ['python'] + sys.argv)

def idle_monitor():
    """Run in a background thread and monitor for idle time."""
    global last_activity_time

    # Wait until last_activity_time has been set at least once
    while last_activity_time is None:
        time.sleep(1)

    logger.info("Idle monitor thread has started.")

    while True:
        if CONFIG.get("enable_idle_restart", False):
            timeout = CONFIG.get("idle_restart_timeout_seconds", 300)

            # If timeout is -1, disable restart checks but still avoid busy loops
            if timeout == -1:
                time.sleep(10)  # Sleep regardless to avoid spinning
                continue

            idle_time = (datetime.now() - last_activity_time).total_seconds()

            if idle_time > timeout:
                logger.info(f"Server idle time ({idle_time:.0f}s) exceeded threshold ({timeout}s).")
                restart_server()
                break  # Exit loop since the process will be replaced

        # Check every 10 seconds
        time.sleep(10)

# --- FastAPI lifecycle events ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle handler executed when the server starts."""
    global idle_monitor_thread, last_activity_time, main_event_loop
    main_event_loop = asyncio.get_running_loop()  # Capture the main event loop
    load_config()  # Load configuration first
    
    # --- Log the current operating mode ---
    mode = CONFIG.get("id_updater_last_mode", "direct_chat")
    target = CONFIG.get("id_updater_battle_target", "A")
    logger.info("="*60)
    logger.info(f"  Active mode: {mode.upper()}")
    if mode == 'battle':
        logger.info(f"  - Battle target: Assistant {target}")
    logger.info("  (Run id_updater.py to change the mode)")
    logger.info("="*60)

    check_for_updates()  # Look for program updates
    load_model_map()  # Refresh the model mapping
    load_model_endpoint_map()  # Load per-model endpoint configuration
    logger.info("Server startup complete. Waiting for the Tampermonkey script to connect...")

    # Display announcements last so they stand out in the logs
    check_and_display_announcement()

    # After reloading the models, reset the activity timestamp baseline
    last_activity_time = datetime.now()
    
    # Kick off the idle-monitoring thread
    if CONFIG.get("enable_idle_restart", False):
        idle_monitor_thread = threading.Thread(target=idle_monitor, daemon=True)
        idle_monitor_thread.start()
        

    yield
    logger.info("Server is shutting down.")

app = FastAPI(lifespan=lifespan)

# --- CORS middleware configuration ---
# Allow all origins, methods, and headers. This is safe for local tooling.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper functions ---
def save_config():
    """Persist the current CONFIG object back to config.jsonc while preserving comments."""
    try:
        # Read the original file so we can preserve comments
        with open('config.jsonc', 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Use a regex-based replacement to update the value safely
        def replacer(key, value, content):
            # This pattern finds the key and matches its value up to the comma or closing brace
            pattern = re.compile(rf'("{key}"\s*:\s*").*?("?)(,?\s*)$', re.MULTILINE)
            replacement = rf'\g<1>{value}\g<2>\g<3>'
            if not pattern.search(content):  # If the key is missing, append it at the end
                 content = re.sub(r'}\s*$', f'  ,"{key}": "{value}"\n}}', content)
            else:
                 content = pattern.sub(replacement, content)
            return content

        content_str = "".join(lines)
        content_str = replacer("session_id", CONFIG["session_id"], content_str)
        content_str = replacer("message_id", CONFIG["message_id"], content_str)
        
        with open('config.jsonc', 'w', encoding='utf-8') as f:
            f.write(content_str)
        logger.info("✅ Session information written to config.jsonc successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to write config.jsonc: {e}", exc_info=True)


async def _process_openai_message(message: dict) -> dict:
    """
    Handle an OpenAI message and split text from attachments.
    - Break multimodal content into plain text and attachment lists.
    - File-bed handling occurs earlier; this function only builds standard attachments.
    - Ensure an empty user message becomes a space to avoid LMArena errors.
    """
    content = message.get("content")
    role = message.get("role")
    attachments = []
    text_content = ""

    if isinstance(content, list):
        text_parts = []
        for part in content:
            if part.get("type") == "text":
                text_parts.append(part.get("text", ""))
            elif part.get("type") == "image_url":
                # The URL can be a base64 string or an HTTP URL (already pre-processed)
                image_url_data = part.get("image_url", {})
                url = image_url_data.get("url")
                original_filename = image_url_data.get("detail")

                try:
                    # Extract the content type when the data is base64 encoded
                    if url.startswith("data:"):
                        content_type = url.split(';')[0].split(':')[1]
                    else:
                        # Guess the content type for HTTP URLs
                        content_type = mimetypes.guess_type(url)[0] or 'application/octet-stream'

                    file_name = original_filename or f"image_{uuid.uuid4()}.{mimetypes.guess_extension(content_type).lstrip('.') or 'png'}"
                    
                    attachments.append({
                        "name": file_name,
                        "contentType": content_type,
                        "url": url
                    })

                except (AttributeError, IndexError, ValueError) as e:
                    logger.warning(f"Error processing attachment URL: {url[:100]}... Details: {e}")

        text_content = "\n\n".join(text_parts)
    elif isinstance(content, str):
        text_content = content

    if role == "user" and not text_content.strip():
        text_content = " "

    return {
        "role": role,
        "content": text_content,
        "attachments": attachments
    }

async def convert_openai_to_lmarena_payload(openai_data: dict, session_id: str, message_id: str, mode_override: str = None, battle_target_override: str = None) -> dict:
    """
    Convert an OpenAI request body into the simplified payload required by the userscript.
    Applies Tavern Mode, Bypass Mode, and battle-mode tweaks.
    Supports per-model overrides for mode and battle target.
    """
    # 1. Normalize roles and process messages
    #    - Convert non-standard 'developer' roles into 'system' for compatibility.
    #    - Separate text and attachments.
    messages = openai_data.get("messages", [])
    for msg in messages:
        if msg.get("role") == "developer":
            msg["role"] = "system"
            logger.info("Normalized message role: converted 'developer' to 'system'.")
            
    processed_messages = []
    for msg in messages:
        processed_msg = await _process_openai_message(msg.copy())
        processed_messages.append(processed_msg)

    # 2. Apply Tavern Mode
    if CONFIG.get("tavern_mode_enabled"):
        system_prompts = [msg['content'] for msg in processed_messages if msg['role'] == 'system']
        other_messages = [msg for msg in processed_messages if msg['role'] != 'system']
        
        merged_system_prompt = "\n\n".join(system_prompts)
        final_messages = []
        
        if merged_system_prompt:
            # System messages should not contain attachments
            final_messages.append({"role": "system", "content": merged_system_prompt, "attachments": []})
        
        final_messages.extend(other_messages)
        processed_messages = final_messages

    # 3. Determine the target model ID
    model_name = openai_data.get("model", "claude-3-5-sonnet-20241022")
    model_info = MODEL_NAME_TO_ID_MAP.get(model_name, {})  # Ensure model_info is always a dictionary
    
    target_model_id = None
    if model_info:
        target_model_id = model_info.get("id")
    else:
        logger.warning(f"Model '{model_name}' is not present in 'models.json'. The request will omit a specific model ID.")

    if not target_model_id:
        logger.warning(f"Model '{model_name}' missing ID in models.json. Request will proceed without an explicit model ID.")

    # 4. Build message templates
    message_templates = []
    for msg in processed_messages:
        message_templates.append({
            "role": msg["role"],
            "content": msg.get("content", ""),
            "attachments": msg.get("attachments", [])
        })
    
    # 4.5. Special handling: create a mock assistant reply if the last user message ends with --bypass and carries images
    if message_templates and message_templates[-1]["role"] == "user":
        last_msg = message_templates[-1]
        if last_msg["content"].strip().endswith("--bypass") and last_msg.get("attachments"):
            has_images = False
            for attachment in last_msg.get("attachments", []):
                if attachment.get("contentType", "").startswith("image/"):
                    has_images = True
                    break
            
            if has_images:
                logger.info("Detected --bypass tag with image attachments; generating a mock assistant reply.")
                
                # Remove the --bypass marker from the user message
                last_msg["content"] = last_msg["content"].strip()[:-9].strip()
                
                # Build a fake assistant message using the user's image attachments
                fake_assistant_msg = {
                    "role": "assistant",
                    "content": "",  # Empty placeholder content
                    "attachments": last_msg.get("attachments", []).copy()  # Copy the user's image attachments
                }
                
                # Clear attachments from the original user message
                last_msg["attachments"] = []
                
                # Insert the fake assistant message just before the user message
                message_templates.insert(len(message_templates)-1, fake_assistant_msg)
                
                # If the conversation starts with an assistant message, prepend a placeholder user message
                if message_templates[0]["role"] == "assistant":
                    logger.info("Conversation starts with assistant; prepending placeholder user message...")
                    fake_user_msg = {
                        "role": "user",
                        "content": "Hi",
                        "attachments": []
                    }
                    message_templates.insert(0, fake_user_msg)

    # 5. Apply Bypass Mode (text models only)
    model_type = model_info.get("type", "text")
    if CONFIG.get("bypass_enabled") and model_type == "text":
        # Always append a position 'a' user message when bypass mode is enabled
        logger.info("Bypass mode enabled; injecting a blank user message.")
        message_templates.append({"role": "user", "content": " ", "participantPosition": "a", "attachments": []})

    # 6. Apply participant positions
    #    - Prefer mode overrides, otherwise fall back to global defaults
    mode = mode_override or CONFIG.get("id_updater_last_mode", "direct_chat")
    target_participant = battle_target_override or CONFIG.get("id_updater_battle_target", "A")
    target_participant = target_participant.lower()  # Normalize to lowercase

    logger.info(f"Configuring participant positions for mode '{mode}' (target: {target_participant if mode == 'battle' else 'N/A'})...")

    for msg in message_templates:
        if msg['role'] == 'system':
            if mode == 'battle':
                # Battle mode: align system messages with the selected assistant side
                msg['participantPosition'] = target_participant
            else:
                # DirectChat mode: system messages always use side 'b'
                msg['participantPosition'] = 'b'
        elif mode == 'battle':
            # In battle mode, non-system messages use the chosen participant side
            msg['participantPosition'] = target_participant
        else:  # DirectChat mode
            # In DirectChat, non-system messages always use side 'a'
            msg['participantPosition'] = 'a'

    return {
        "message_templates": message_templates,
        "target_model_id": target_model_id,
        "session_id": session_id,
        "message_id": message_id
    }

# --- OpenAI formatting helpers (ensure robust JSON serialization) ---
def format_openai_chunk(content: str, model: str, request_id: str) -> str:
    """Format a streaming chunk compatible with the OpenAI API."""
    chunk = {
        "id": request_id, "object": "chat.completion.chunk",
        "created": int(time.time()), "model": model,
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

def format_openai_finish_chunk(model: str, request_id: str, reason: str = 'stop') -> str:
    """Format the final streaming chunk that signals completion."""
    chunk = {
        "id": request_id, "object": "chat.completion.chunk",
        "created": int(time.time()), "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\ndata: [DONE]\n\n"

def format_openai_error_chunk(error_message: str, model: str, request_id: str) -> str:
    """Format a streaming chunk that conveys an error message."""
    content = f"\n\n[LMArena Bridge Error]: {error_message}"
    return format_openai_chunk(content, model, request_id)

def format_openai_non_stream_response(content: str, model: str, request_id: str, reason: str = 'stop') -> dict:
    """Build a non-streaming response body that matches the OpenAI schema."""
    return {
        "id": request_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": reason,
        }],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": len(content) // 4,
            "total_tokens": len(content) // 4,
        },
    }

async def _process_lmarena_stream(request_id: str):
    """
    Core internal generator: process raw browser data and emit structured events.
    Event types: ('content', str), ('finish', str), ('error', str)
    """
    global IS_REFRESHING_FOR_VERIFICATION
    queue = response_channels.get(request_id)
    if not queue:
        logger.error(f"PROCESSOR [ID: {request_id[:8]}]: Response channel not found.")
        yield 'error', 'Internal server error: response channel not found.'
        return

    buffer = ""
    timeout = CONFIG.get("stream_response_timeout_seconds",360)
    text_pattern = re.compile(r'[ab]0:"((?:\\.|[^"\\])*)"')
    # Regex used to match and extract image URLs
    image_pattern = re.compile(r'[ab]2:(\[.*?\])')
    finish_pattern = re.compile(r'[ab]d:(\{.*?"finishReason".*?\})')
    error_pattern = re.compile(r'(\{\s*"error".*?\})', re.DOTALL)
    cloudflare_patterns = [r'<title>Just a moment...</title>', r'Enable JavaScript and cookies to continue']
    
    has_yielded_content = False  # Track whether valid content has already been yielded

    try:
        while True:
            try:
                raw_data = await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: Timed out waiting for browser data ({timeout}s).")
                yield 'error', f'Response timed out after {timeout} seconds.'
                return

            # --- Cloudflare verification handling ---
            def handle_cloudflare_verification():
                global IS_REFRESHING_FOR_VERIFICATION
                if not IS_REFRESHING_FOR_VERIFICATION:
                    logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: Detected a verification challenge; sending refresh command.")
                    IS_REFRESHING_FOR_VERIFICATION = True
                    if browser_ws:
                        asyncio.create_task(browser_ws.send_text(json.dumps({"command": "refresh"}, ensure_ascii=False)))
                    return "Verification detected. A refresh command was sent—please try again in a moment."
                else:
                    logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Verification still in progress; waiting for the refresh to finish.")
                    return "Waiting for the verification challenge to finish..."

            # 1. Inspect direct errors coming from the WebSocket channel
            if isinstance(raw_data, dict) and 'error' in raw_data:
                error_msg = raw_data.get('error', 'Unknown browser error')
                if isinstance(error_msg, str):
                    if '413' in error_msg or 'too large' in error_msg.lower():
                        friendly_error_msg = "Upload failed: the attachment exceeds LMArena's size limit (usually about 5 MB). Try compressing the file or uploading a smaller one."
                        logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: Attachment too large (413).")
                        yield 'error', friendly_error_msg
                        return
                    if any(re.search(p, error_msg, re.IGNORECASE) for p in cloudflare_patterns):
                        yield 'error', handle_cloudflare_verification()
                        return
                yield 'error', error_msg
                return

            # 2. Check for the [DONE] signal
            if raw_data == "[DONE]":
                # State reset logic moved to websocket_endpoint so reconnects always reset cleanly
                if has_yielded_content and IS_REFRESHING_FOR_VERIFICATION:
                     logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Request succeeded; verification state will reset on the next connection.")
                break

            # 3. Append to the buffer and inspect the accumulated data
            buffer += "".join(str(item) for item in raw_data) if isinstance(raw_data, list) else raw_data

            if any(re.search(p, buffer, re.IGNORECASE) for p in cloudflare_patterns):
                yield 'error', handle_cloudflare_verification()
                return
            
            if (error_match := error_pattern.search(buffer)):
                try:
                    error_json = json.loads(error_match.group(1))
                    yield 'error', error_json.get("error", "Unknown error returned by LMArena")
                    return
                except json.JSONDecodeError: pass

            # Prefer handling text chunks first
            while (match := text_pattern.search(buffer)):
                try:
                    text_content = json.loads(f'"{match.group(1)}"')
                    if text_content:
                        has_yielded_content = True
                        yield 'content', text_content
                except (ValueError, json.JSONDecodeError): pass
                buffer = buffer[match.end():]

            # Handle image payloads
            while (match := image_pattern.search(buffer)):
                try:
                    image_data_list = json.loads(match.group(1))
                    if isinstance(image_data_list, list) and image_data_list:
                        image_info = image_data_list[0]
                        if image_info.get("type") == "image" and "image" in image_info:
                            # Wrap the URL in Markdown and yield as a content block
                            markdown_image = f"![Image]({image_info['image']})"
                            yield 'content', markdown_image
                except (json.JSONDecodeError, IndexError) as e:
                    logger.warning(f"Failed to parse image URL: {e}, buffer: {buffer[:150]}")
                buffer = buffer[match.end():]

            if (finish_match := finish_pattern.search(buffer)):
                try:
                    finish_data = json.loads(finish_match.group(1))
                    yield 'finish', finish_data.get("finishReason", "stop")
                except (json.JSONDecodeError, IndexError): pass
                buffer = buffer[finish_match.end():]

    except asyncio.CancelledError:
        logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Task cancelled.")
    finally:
        if request_id in response_channels:
            del response_channels[request_id]
            logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Response channel cleaned up.")

async def stream_generator(request_id: str, model: str):
    """Format internal events as an OpenAI-compatible SSE response."""
    response_id = f"chatcmpl-{uuid.uuid4()}"
    logger.info(f"STREAMER [ID: {request_id[:8]}]: Streaming generator started.")

    finish_reason_to_send = 'stop'  # Default finishing reason

    async for event_type, data in _process_lmarena_stream(request_id):
        if event_type == 'content':
            yield format_openai_chunk(data, model, response_id)
        elif event_type == 'finish':
            # Remember the finish reason but wait for the browser to send [DONE]
            finish_reason_to_send = data
            if data == 'content-filter':
                warning_msg = "\n\nThe response was terminated, likely due to context length or internal moderation."
                yield format_openai_chunk(warning_msg, model, response_id)
        elif event_type == 'error':
            logger.error(f"STREAMER [ID: {request_id[:8]}]: Error while streaming: {data}")
            yield format_openai_error_chunk(str(data), model, response_id)
            yield format_openai_finish_chunk(model, response_id, reason='stop')
            return  # Stop immediately on error

    # Only run after _process_lmarena_stream finishes naturally (i.e., receives [DONE])
    yield format_openai_finish_chunk(model, response_id, reason=finish_reason_to_send)
    logger.info(f"STREAMER [ID: {request_id[:8]}]: Streaming generator finished normally.")

async def non_stream_response(request_id: str, model: str):
    """Aggregate events into a single OpenAI-style JSON response."""
    response_id = f"chatcmpl-{uuid.uuid4()}"
    logger.info(f"NON-STREAM [ID: {request_id[:8]}]: Handling non-streaming response.")
    
    full_content = []
    finish_reason = "stop"
    
    async for event_type, data in _process_lmarena_stream(request_id):
        if event_type == 'content':
            full_content.append(data)
        elif event_type == 'finish':
            finish_reason = data
            if data == 'content-filter':
                full_content.append("\n\nThe response was terminated, likely due to context length or internal moderation.")
            # Do not break here; keep waiting for [DONE] to avoid race conditions
        elif event_type == 'error':
            logger.error(f"NON-STREAM [ID: {request_id[:8]}]: Error while processing: {data}")

            # Align streaming and non-streaming error status codes
            status_code = 413 if "exceeds LMArena's size limit" in str(data) else 500

            error_response = {
                "error": {
                    "message": f"[LMArena Bridge Error]: {data}",
                    "type": "bridge_error",
                    "code": "attachment_too_large" if status_code == 413 else "processing_error"
                }
            }
            return Response(content=json.dumps(error_response, ensure_ascii=False), status_code=status_code, media_type="application/json")

    final_content = "".join(full_content)
    response_data = format_openai_non_stream_response(final_content, model, response_id, reason=finish_reason)
    
    logger.info(f"NON-STREAM [ID: {request_id[:8]}]: Response aggregation complete.")
    return Response(content=json.dumps(response_data, ensure_ascii=False), media_type="application/json")

# --- WebSocket endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle the WebSocket connection from the userscript."""
    global browser_ws, IS_REFRESHING_FOR_VERIFICATION
    await websocket.accept()
    if browser_ws is not None:
        logger.warning("New userscript connection detected; replacing the previous connection.")
    
    # Any new connection implies the verification flow has ended (or never started)
    if IS_REFRESHING_FOR_VERIFICATION:
        logger.info("✅ New WebSocket connection established; verification state reset.")
        IS_REFRESHING_FOR_VERIFICATION = False
        
    logger.info("✅ Userscript WebSocket connected successfully.")
    browser_ws = websocket
    try:
        while True:
            # Wait for messages from the userscript
            message_str = await websocket.receive_text()
            message = json.loads(message_str)
            
            request_id = message.get("request_id")
            data = message.get("data")

            if not request_id or data is None:
                logger.warning(f"Invalid message received from browser: {message}")
                continue

            # Route the payload into the matching response channel
            if request_id in response_channels:
                await response_channels[request_id].put(data)
            else:
                logger.warning(f"⚠️ Received response for unknown or closed request: {request_id}")

    except WebSocketDisconnect:
        logger.warning("❌ Userscript client disconnected.")
    except Exception as e:
        logger.error(f"Unexpected error while handling WebSocket: {e}", exc_info=True)
    finally:
        browser_ws = None
        # Clean up any pending response channels to avoid hanging requests
        for queue in response_channels.values():
            await queue.put({"error": "Browser disconnected during operation"})
        response_channels.clear()
        logger.info("WebSocket connection cleaned up.")

# --- OpenAI-compatible API endpoints ---
@app.get("/v1/models")
async def get_models():
    """Return the list of OpenAI-compatible models."""
    if not MODEL_NAME_TO_ID_MAP:
        return JSONResponse(
            status_code=404,
            content={"error": "Model list is empty or 'models.json' is missing."}
        )
    
    return {
        "object": "list",
        "data": [
            {
                "id": model_name, 
                "object": "model",
                "created": int(time.time()),
                "owned_by": "LMArenaBridge"
            }
            for model_name in MODEL_NAME_TO_ID_MAP.keys()
        ],
    }

@app.post("/internal/request_model_update")
async def request_model_update():
    """
    Receive a request from model_updater.py and instruct the userscript
    to send the page source via WebSocket.
    """
    if not browser_ws:
        logger.warning("MODEL UPDATE: Update requested but no browser connection is available.")
        raise HTTPException(status_code=503, detail="Browser client not connected.")

    try:
        logger.info("MODEL UPDATE: Forwarding request via WebSocket...")
        await browser_ws.send_text(json.dumps({"command": "send_page_source"}))
        logger.info("MODEL UPDATE: 'send_page_source' command sent successfully.")
        return JSONResponse({"status": "success", "message": "Request to send page source sent."})
    except Exception as e:
        logger.error(f"MODEL UPDATE: Failed to send command: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to send command via WebSocket.")

@app.post("/internal/update_available_models")
async def update_available_models_endpoint(request: Request):
    """
    Receive page HTML from the userscript and update available_models.json.
    """
    html_content = await request.body()
    if not html_content:
        logger.warning("MODEL UPDATE: No HTML content received in the request.")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "No HTML content received."}
        )
    
    logger.info("MODEL UPDATE: HTML received; extracting available models...")
    new_models_list = extract_models_from_html(html_content.decode('utf-8'))
    
    if new_models_list:
        save_available_models(new_models_list)
        return JSONResponse({"status": "success", "message": "Available models file updated."})
    else:
        logger.error("MODEL UPDATE: Failed to extract model data from the supplied HTML.")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Could not extract model data from HTML."}
        )


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """
    Handle chat completion requests.
    Accepts an OpenAI-formatted payload, converts it into the LMArena format,
    relays it via WebSocket to the userscript, and streams results back.
    """
    global last_activity_time
    last_activity_time = datetime.now()  # Update last activity timestamp
    logger.info(f"API request received; activity time updated to {last_activity_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        openai_req = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    model_name = openai_req.get("model")
    model_info = MODEL_NAME_TO_ID_MAP.get(model_name, {})  # Ensure a dict even when missing
    model_type = model_info.get("type", "text")  # Default to text

    # --- Model-type specific handling ---
    if model_type == 'image':
        logger.info(f"Model '{model_name}' is of type 'image'; using the unified chat path.")
        # Image models now reuse the main chat logic because
        # _process_lmarena_stream can handle image payloads directly.
        # This brings native streaming and non-streaming parity for images.
        pass  # Continue into the common chat logic below
    # --- Image handling complete ---

    # Normal text handling continues below
    load_config()  # Reload config to keep session data fresh
    # --- API key verification ---
    api_key = CONFIG.get("api_key")
    if api_key:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(
                status_code=401,
                detail="API key required. Provide it in the Authorization header as 'Bearer YOUR_KEY'."
            )
        
        provided_key = auth_header.split(' ')[1]
        if provided_key != api_key:
            raise HTTPException(
                status_code=401,
                detail="The provided API key is incorrect."
            )

    # --- Enhanced connection checks (handles post-verification race conditions) ---
    if IS_REFRESHING_FOR_VERIFICATION and not browser_ws:
        raise HTTPException(
            status_code=503,
            detail="Waiting for the browser to refresh after verification. Please try again in a few seconds."
        )

    if not browser_ws:
        raise HTTPException(
            status_code=503,
            detail="Userscript client not connected. Ensure an LMArena tab is open with the script active."
        )

    # --- Model-to-session mapping logic ---
    session_id, message_id = None, None
    mode_override, battle_target_override = None, None

    if model_name and model_name in MODEL_ENDPOINT_MAP:
        mapping_entry = MODEL_ENDPOINT_MAP[model_name]
        selected_mapping = None

        if isinstance(mapping_entry, list) and mapping_entry:
            selected_mapping = random.choice(mapping_entry)
            logger.info(f"Model '{model_name}' picked a random mapping from its pool.")
        elif isinstance(mapping_entry, dict):
            selected_mapping = mapping_entry
            logger.info(f"Model '{model_name}' is using a single legacy endpoint mapping.")
        
        if selected_mapping:
            session_id = selected_mapping.get("session_id")
            message_id = selected_mapping.get("message_id")
            # Capture mode overrides along with the session identifiers
            mode_override = selected_mapping.get("mode")  # May be None
            battle_target_override = selected_mapping.get("battle_target")  # May be None
            log_msg = f"Using Session ID: ...{session_id[-6:] if session_id else 'N/A'}"
            if mode_override:
                log_msg += f" (mode: {mode_override}"
                if mode_override == 'battle':
                    log_msg += f", target: {battle_target_override or 'A'}"
                log_msg += ")"
            logger.info(log_msg)

    # Fall back to global defaults if no mapping was resolved
    if not session_id:
        if CONFIG.get("use_default_ids_if_mapping_not_found", True):
            session_id = CONFIG.get("session_id")
            message_id = CONFIG.get("message_id")
            # Global IDs should honor the global configuration
            mode_override, battle_target_override = None, None
            logger.info(f"Model '{model_name}' fell back to the global Session ID: ...{session_id[-6:] if session_id else 'N/A'}")
        else:
            logger.error(f"Model '{model_name}' has no mapping in 'model_endpoint_map.json' and fallback is disabled.")
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Model '{model_name}' has no dedicated session mapping. Add one to 'model_endpoint_map.json' "
                    "or enable 'use_default_ids_if_mapping_not_found' in config.jsonc."
                )
            )

    # --- Validate the finalized session identifiers ---
    if not session_id or not message_id or "YOUR_" in session_id or "YOUR_" in message_id:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid session_id or message_id. Review 'model_endpoint_map.json' and 'config.jsonc', "
                "or run `id_updater.py` to refresh the defaults."
            )
        )

    if not model_name or model_name not in MODEL_NAME_TO_ID_MAP:
        logger.warning(f"Requested model '{model_name}' not found in models.json; using default model ID.")

    request_id = str(uuid.uuid4())
    response_channels[request_id] = asyncio.Queue()
    logger.info(f"API CALL [ID: {request_id[:8]}]: Response channel created.")

    try:
        # --- Attachment preprocessing (including file-bed uploads) ---
        # Prepare all attachments before talking to the browser; fail fast if anything goes wrong.
        messages_to_process = openai_req.get("messages", [])
        for message in messages_to_process:
            content = message.get("content")
            if isinstance(content, list):
                for i, part in enumerate(content):
                    if part.get("type") == "image_url" and CONFIG.get("file_bed_enabled"):
                        image_url_data = part.get("image_url", {})
                        base64_url = image_url_data.get("url")
                        original_filename = image_url_data.get("detail")
                        
                        if not (base64_url and base64_url.startswith("data:")):
                            raise ValueError(f"Invalid image data format: {base64_url[:100] if base64_url else 'None'}")

                        upload_url = CONFIG.get("file_bed_upload_url")
                        if not upload_url:
                            raise ValueError("File bed enabled but 'file_bed_upload_url' is not configured.")
                        
                        # Normalise escaped slashes
                        upload_url = upload_url.replace('\\/', '/')

                        api_key = CONFIG.get("file_bed_api_key")
                        file_name = original_filename or f"image_{uuid.uuid4()}.png"
                        
                        logger.info(f"File bed preprocessing: uploading '{file_name}'...")
                        uploaded_filename, error_message = await upload_to_file_bed(file_name, base64_url, upload_url, api_key)

                        if error_message:
                            raise IOError(f"File bed upload failed: {error_message}")
                        
                        # Build the final URL using the configured prefix
                        url_prefix = upload_url.rsplit('/', 1)[0]
                        final_url = f"{url_prefix}/uploads/{uploaded_filename}"
                        
                        part["image_url"]["url"] = final_url
                        logger.info(f"Attachment URL replaced with: {final_url}")

        # 1. Convert the request (attachments already handled)
        lmarena_payload = await convert_openai_to_lmarena_payload(
            openai_req,
            session_id,
            message_id,
            mode_override=mode_override,
            battle_target_override=battle_target_override
        )
        
        # Explicitly flag image requests for the userscript
        if model_type == 'image':
            lmarena_payload['is_image_request'] = True
        
        # 2. Package the payload for the browser
        message_to_browser = {
            "request_id": request_id,
            "payload": lmarena_payload
        }
        
        # 3. Send over WebSocket
        logger.info(f"API CALL [ID: {request_id[:8]}]: Sending payload to the userscript via WebSocket.")
        await browser_ws.send_text(json.dumps(message_to_browser))

        # 4. Honour the stream flag
        is_stream = openai_req.get("stream", False)

        if is_stream:
            # Return a streaming response
            return StreamingResponse(
                stream_generator(request_id, model_name or "default_model"),
                media_type="text/event-stream"
            )
        else:
            # Return a non-streaming response
            return await non_stream_response(request_id, model_name or "default_model")
    except (ValueError, IOError) as e:
        # Attachment preprocessing error
        logger.error(f"API CALL [ID: {request_id[:8]}]: Attachment preprocessing failed: {e}")
        if request_id in response_channels:
            del response_channels[request_id]
        # Return a well-formed JSON error response
        return JSONResponse(
            status_code=500,
            content={"error": {"message": f"[LMArena Bridge Error] Attachment preprocessing failed: {e}", "type": "attachment_error"}}
        )
    except Exception as e:
        # Catch-all for unexpected errors
        if request_id in response_channels:
            del response_channels[request_id]
        logger.error(f"API CALL [ID: {request_id[:8]}]: Fatal error while processing: {e}", exc_info=True)
        # Ensure we still return a proper JSON payload
        return JSONResponse(
            status_code=500,
            content={"error": {"message": str(e), "type": "internal_server_error"}}
        )

# --- Internal coordination endpoints ---
@app.post("/internal/start_id_capture")
async def start_id_capture():
    """
    Receive notifications from id_updater.py and instruct the userscript
    to activate ID capture mode via WebSocket.
    """
    if not browser_ws:
        logger.warning("ID CAPTURE: Activation requested but no browser is connected.")
        raise HTTPException(status_code=503, detail="Browser client not connected.")
    
    try:
        logger.info("ID CAPTURE: Activation requested; sending command through WebSocket...")
        await browser_ws.send_text(json.dumps({"command": "activate_id_capture"}))
        logger.info("ID CAPTURE: Activation command sent successfully.")
        return JSONResponse({"status": "success", "message": "Activation command sent."})
    except Exception as e:
        logger.error(f"ID CAPTURE: Failed to send activation command: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to send command via WebSocket.")


# --- Main entry point ---
if __name__ == "__main__":
    # TODO: consider reading the port from config.jsonc instead of hardcoding
    api_port = 5102
    logger.info(f"🚀 LMArena Bridge v2.0 API server starting up...")
    logger.info(f"   - HTTP address: http://127.0.0.1:{api_port}")
    logger.info(f"   - WebSocket endpoint: ws://127.0.0.1:{api_port}/ws")
    
    uvicorn.run(app, host="0.0.0.0", port=api_port)
