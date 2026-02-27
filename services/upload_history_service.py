"""
Tracks upload file hashes to detect duplicate uploads.
"""
import structlog
from typing import Optional

from config import get_supabase_client

logger = structlog.get_logger(__name__)


class UploadHistoryService:
    def __init__(self):
        self.db = get_supabase_client()
        self.table = "upload_history"

    def check_duplicate(self, upload_type: str, file_hash: str) -> Optional[dict]:
        """Check if this file was previously uploaded. Returns {filename, uploaded_at} or None."""
        result = (
            self.db.table(self.table)
            .select("filename, uploaded_at, row_count")
            .eq("upload_type", upload_type)
            .eq("file_hash", file_hash)
            .order("uploaded_at", desc=True)
            .limit(1)
            .execute()
        )
        return result.data[0] if result.data else None

    def record_upload(
        self,
        upload_type: str,
        file_hash: str,
        filename: str,
        row_count: int = 0,
    ) -> None:
        """Record a successful upload for future duplicate detection."""
        self.db.table(self.table).insert({
            "upload_type": upload_type,
            "file_hash": file_hash,
            "filename": filename,
            "row_count": row_count,
            "status": "success",
        }).execute()
        logger.info(
            "upload_recorded",
            upload_type=upload_type,
            filename=filename,
            row_count=row_count,
        )

    def record_failed_upload(
        self,
        upload_type: str,
        filename: str,
        error_message: str,
        file_hash: str = "",
    ) -> None:
        """Record a failed upload for later diagnosis."""
        # Truncate error message to prevent excessively long entries
        truncated_msg = error_message[:2000] if error_message else "Unknown error"
        try:
            self.db.table(self.table).insert({
                "upload_type": upload_type,
                "file_hash": file_hash or "",
                "filename": filename or "unknown",
                "row_count": 0,
                "status": "error",
                "error_message": truncated_msg,
            }).execute()
            logger.info(
                "failed_upload_recorded",
                upload_type=upload_type,
                filename=filename,
                error=truncated_msg[:200],
            )
        except Exception as log_err:
            # Never let failure logging break the error response
            logger.warning(
                "failed_to_record_upload_error",
                upload_type=upload_type,
                log_error=str(log_err),
            )


_service: Optional[UploadHistoryService] = None


def get_upload_history_service() -> UploadHistoryService:
    global _service
    if _service is None:
        _service = UploadHistoryService()
    return _service
