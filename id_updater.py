# id_updater.py
#
# This is an enhanced one-off HTTP server. It receives session details from
# the userscript based on the chosen mode (DirectChat or Battle) and writes
# them back to config.jsonc.

import http.server
import socketserver
import json
import re
import threading
import os
import requests

# --- Configuration ---
HOST = "127.0.0.1"
PORT = 5103
CONFIG_PATH = 'config.jsonc'

def read_config():
    """Read config.jsonc and strip comments before parsing."""
    if not os.path.exists(CONFIG_PATH):
        print(f"❌ Error: configuration file '{CONFIG_PATH}' does not exist.")
        return None
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Remove comments carefully so URLs containing '//' remain intact
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

        json_content = "".join(no_comments_lines)
        return json.loads(json_content)
    except Exception as e:
        print(f"❌ Failed to read or parse '{CONFIG_PATH}': {e}")
        return None

def save_config_value(key, value):
    """Safely update a single key in config.jsonc while preserving formatting."""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            content = f.read()

        # Use regex to replace the key value safely
        pattern = re.compile(rf'("{key}"\s*:\s*")[^"]*(")')
        new_content, count = pattern.subn(rf'\g<1>{value}\g<2>', content, 1)

        if count == 0:
            print(f"🤔 Warning: key '{key}' not found in '{CONFIG_PATH}'.")
            return False

        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            f.write(new_content)
        return True
    except Exception as e:
        print(f"❌ Failed to update '{CONFIG_PATH}': {e}")
        return False

def save_session_ids(session_id, message_id):
    """Persist the captured session/message IDs to config.jsonc."""
    print(f"\n📝 Attempting to write IDs to '{CONFIG_PATH}'...")
    res1 = save_config_value("session_id", session_id)
    res2 = save_config_value("message_id", message_id)
    if res1 and res2:
        print("✅ IDs updated successfully.")
        print(f"   - session_id: {session_id}")
        print(f"   - message_id: {message_id}")
    else:
        print("❌ Failed to update IDs. Review the errors above.")


class RequestHandler(http.server.SimpleHTTPRequestHandler):
    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_POST(self):
        if self.path == '/update':
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data)

                session_id = data.get('sessionId')
                message_id = data.get('messageId')

                if session_id and message_id:
                    print("\n" + "=" * 50)
                    print("🎉 Successfully captured IDs from the browser!")
                    print(f"  - Session ID: {session_id}")
                    print(f"  - Message ID: {message_id}")
                    print("=" * 50)

                    save_session_ids(session_id, message_id)

                    self.send_response(200)
                    self._send_cors_headers()
                    self.end_headers()
                    self.wfile.write(b'{"status": "success"}')

                    print("\nDone! The server will shut down in one second.")
                    threading.Thread(target=self.server.shutdown).start()

                else:
                    self.send_response(400, "Bad Request")
                    self._send_cors_headers()
                    self.end_headers()
                    self.wfile.write(b'{"error": "Missing sessionId or messageId"}')
            except Exception as e:
                self.send_response(500, "Internal Server Error")
                self._send_cors_headers()
                self.end_headers()
                self.wfile.write(f'{{"error": "Internal server error: {e}"}}'.encode('utf-8'))
        else:
            self.send_response(404, "Not Found")
            self._send_cors_headers()
            self.end_headers()

    def log_message(self, format, *args):
        return

def run_server():
    with socketserver.TCPServer((HOST, PORT), RequestHandler) as httpd:
        print("\n" + "="*50)
        print("  🚀 Session ID capture listener started")
        print(f"  - Listening on: http://{HOST}:{PORT}")
        print("  - Trigger ID capture from an LMArena tab in your browser.")
        print("  - The script will exit automatically once IDs are received.")
        print("="*50)
        httpd.serve_forever()

def notify_api_server():
    """Notify the main API server that ID capture has begun."""
    api_server_url = "http://127.0.0.1:5102/internal/start_id_capture"
    try:
        response = requests.post(api_server_url, timeout=3)
        if response.status_code == 200:
            print("✅ Main server notified to activate ID capture mode.")
            return True
        else:
            print(f"⚠️ Main server responded with status {response.status_code}.")
            print(f"   - Details: {response.text}")
            return False
    except requests.ConnectionError:
        print("❌ Unable to reach the main API server. Ensure api_server.py is running.")
        return False
    except Exception as e:
        print(f"❌ Unexpected error while notifying the main server: {e}")
        return False

if __name__ == "__main__":
    config = read_config()
    if not config:
        exit(1)

    # --- Prompt the user for their preferred mode ---
    last_mode = config.get("id_updater_last_mode", "direct_chat")
    mode_map = {"a": "direct_chat", "b": "battle"}
    
    prompt = f"Select mode [a: DirectChat, b: Battle] (default: {last_mode}): "
    choice = input(prompt).lower().strip()

    if not choice:
        mode = last_mode
    else:
        mode = mode_map.get(choice)
        if not mode:
            print(f"Invalid choice. Using default: {last_mode}")
            mode = last_mode

    save_config_value("id_updater_last_mode", mode)
    print(f"Current mode: {mode.upper()}")
    
    if mode == 'battle':
        last_target = config.get("id_updater_battle_target", "A")
        target_prompt = f"Select which assistant to update [A (required for search models) or B] (default: {last_target}): "
        target_choice = input(target_prompt).upper().strip()

        if not target_choice:
            target = last_target
        elif target_choice in ["A", "B"]:
            target = target_choice
        else:
            print(f"Invalid choice. Using default: {last_target}")
            target = last_target
        
        save_config_value("id_updater_battle_target", target)
        print(f"Battle target: Assistant {target}")
        print("Note: regardless of A or B, the captured IDs overwrite the primary session/message IDs.")

    # Notify the main server before starting the listener
    if notify_api_server():
        run_server()
        print("Server stopped.")
    else:
        print("\nID capture aborted because the main server could not be notified.")
