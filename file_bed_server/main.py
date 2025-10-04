# file_bed_server/main.py
import base64
import os
import uuid
import time
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import logging
from apscheduler.schedulers.background import BackgroundScheduler

# --- Basic configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Path configuration ---
# Store uploaded files alongside main.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
API_KEY = "your_secret_api_key"  # Lightweight API key check
CLEANUP_INTERVAL_MINUTES = 1  # Cleanup frequency in minutes
FILE_MAX_AGE_MINUTES = 10  # Maximum file retention in minutes

# --- Cleanup routine ---
def cleanup_old_files():
    """Remove uploaded files older than the configured retention window."""
    now = time.time()
    cutoff = now - (FILE_MAX_AGE_MINUTES * 60)
    
    logger.info(f"Running cleanup: deleting files older than {datetime.fromtimestamp(cutoff).strftime('%Y-%m-%d %H:%M:%S')}...")
    
    deleted_count = 0
    try:
        for filename in os.listdir(UPLOAD_DIR):
            file_path = os.path.join(UPLOAD_DIR, filename)
            if os.path.isfile(file_path):
                try:
                    file_mtime = os.path.getmtime(file_path)
                    if file_mtime < cutoff:
                        os.remove(file_path)
                        logger.info(f"Deleted expired file: {filename}")
                        deleted_count += 1
                except OSError as e:
                    logger.error(f"Error deleting file '{file_path}': {e}")
    except Exception as e:
        logger.error(f"Unexpected error while cleaning files: {e}", exc_info=True)

    if deleted_count > 0:
        logger.info(f"Cleanup finished. Deleted {deleted_count} files.")
    else:
        logger.info("Cleanup finished. No files needed removal.")


# --- FastAPI lifecycle events ---
scheduler = BackgroundScheduler(timezone="UTC")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the cleanup scheduler on startup and shut it down gracefully."""
    # Start scheduler and register cleanup job
    scheduler.add_job(cleanup_old_files, 'interval', minutes=CLEANUP_INTERVAL_MINUTES)
    scheduler.start()
    logger.info(f"Background cleanup started; running every {CLEANUP_INTERVAL_MINUTES} minutes.")
    yield
    # Shut down scheduler
    scheduler.shutdown()
    logger.info("Background cleanup stopped.")


app = FastAPI(lifespan=lifespan)

# --- Ensure the upload directory exists ---
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)
    logger.info(f"Created upload directory '{UPLOAD_DIR}'.")

# --- Mount static route for uploaded files ---
app.mount(f"/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")

# --- Pydantic model definition ---
class UploadRequest(BaseModel):
    file_name: str
    file_data: str  # Full base64 data URI
    api_key: str | None = None

# --- API endpoint ---
@app.post("/upload")
async def upload_file(request: UploadRequest, http_request: Request):
    """Accept a base64-encoded file, persist it, and return the public URL."""
    # Basic API key validation
    if API_KEY and request.api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    try:
        # 1. Parse the base64 data URI
        header, encoded_data = request.file_data.split(',', 1)
        
        # 2. Decode the base64 payload
        file_data = base64.b64decode(encoded_data)
        
        # 3. Generate a unique filename to avoid collisions
        file_extension = os.path.splitext(request.file_name)[1]
        if not file_extension:
            # Infer extension from the MIME type in the header
            import mimetypes
            mime_type = header.split(';')[0].split(':')[1]
            guessed_extension = mimetypes.guess_extension(mime_type)
            file_extension = guessed_extension if guessed_extension else '.bin'

        unique_filename = f"{uuid.uuid4()}{file_extension}"
        file_path = os.path.join(UPLOAD_DIR, unique_filename)

        # 4. Write the file to disk
        with open(file_path, "wb") as f:
            f.write(file_data)
        
        # 5. Return success info and generated filename
        logger.info(f"Uploaded '{request.file_name}' as '{unique_filename}'.")
        
        return JSONResponse(
            status_code=200,
            content={"success": True, "filename": unique_filename}
        )

    except (ValueError, IndexError) as e:
        logger.error(f"Failed to parse base64 data: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid base64 data URI format: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while processing upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

@app.get("/")
def read_root():
    return {"message": "LMArena Bridge file bed server is running."}

# --- Main entry point ---
if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 File bed server starting up...")
    logger.info("   - HTTP address: http://127.0.0.1:5180")
    logger.info(f"   - Upload endpoint: http://127.0.0.1:5180/upload")
    logger.info(f"   - File access path: /uploads")
    uvicorn.run(app, host="0.0.0.0", port=5180)
