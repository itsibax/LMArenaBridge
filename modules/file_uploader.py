# modules/file_uploader.py
import httpx
import logging

logger = logging.getLogger(__name__)

from typing import Tuple

from typing import Tuple

async def upload_to_file_bed(file_name: str, file_data: str, upload_url: str, api_key: str | None = None) -> Tuple[str | None, str | None]:
    """Upload a base64-encoded file to the file-bed service.

    :param file_name: Original filename supplied by the client.
    :param file_data: Base64 data URI (for example, "data:image/png;base64,...").
    :param upload_url: The /upload endpoint of the file-bed server.
    :param api_key: Optional API key for authentication.
    :return: Tuple (filename, error_message). On success filename is a string and
             error_message is None; on failure filename is None and error_message
             contains the error details.
    """
    payload = {
        "file_name": file_name,
        "file_data": file_data,
        "api_key": api_key
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(upload_url, json=payload)
            
            response.raise_for_status()  # Raise for any 4xx or 5xx responses
            
            result = response.json()
            if result.get("success") and result.get("filename"):
                logger.info(f"Uploaded '{file_name}' to file bed as {result['filename']}")
                return result["filename"], None
            else:
                error_msg = result.get("error", "File bed returned an unknown error.")
                logger.error(f"File bed upload failed: {error_msg}")
                return None, error_msg
                
    except httpx.HTTPStatusError as e:
        error_details = f"HTTP error: {e.response.status_code} - {e.response.text}"
        logger.error(f"File bed upload encountered {error_details}")
        return None, error_details
    except httpx.RequestError as e:
        error_details = f"Connection error: {e}"
        logger.error(f"Failed to connect to the file-bed server: {e}")
        return None, error_details
    except Exception as e:
        error_details = f"Unexpected error: {e}"
        logger.error(f"Unexpected error while uploading file: {e}", exc_info=True)
        return None, error_details
