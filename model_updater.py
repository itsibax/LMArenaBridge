# model_updater.py
import requests
import time
import logging

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
API_SERVER_URL = "http://127.0.0.1:5102"  # Must match the port used in api_server.py

def trigger_model_update():
    """Notify the main server to start the model list refresh workflow."""
    try:
        logging.info("Requesting model list refresh from the main server...")
        response = requests.post(f"{API_SERVER_URL}/internal/request_model_update")
        response.raise_for_status()
        
        if response.json().get("status") == "success":
            logging.info("✅ Update request accepted. The server will refresh the model list.")
            logging.info("Keep an LMArena tab open so the userscript can extract the latest models.")
            logging.info("The refreshed list will be stored in `available_models.json`.")
        else:
            logging.error(f"❌ Server responded with an error: {response.json().get('message')}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Could not connect to the main server ({API_SERVER_URL}).")
        logging.error("Ensure that `api_server.py` is running.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    trigger_model_update()
    # Allow logs to flush before the script exits
    time.sleep(2)
