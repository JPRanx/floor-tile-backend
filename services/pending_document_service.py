"""
Pending document service for unmatched document queue management.

Handles storing unmatched documents in Supabase DB and Storage,
and resolving them via manual assignment.
"""

import uuid
from typing import Optional
from datetime import datetime, timezone
import structlog

from config import get_supabase_client
from models.pending_document import (
    PendingDocumentCreate,
    PendingDocumentResponse,
    ResolvePendingRequest,
    PendingStatus,
    ResolvedAction,
)
from models.ingest import ParsedDocumentData, ConfirmIngestRequest
from exceptions import NotFoundError, DatabaseError

logger = structlog.get_logger(__name__)

# Storage bucket for pending PDFs
STORAGE_BUCKET = "documents"
STORAGE_PREFIX = "pending"


class PendingDocumentService:
    """
    Service for managing pending (unmatched) documents.

    Handles:
    - Storing pending documents with PDF in Supabase Storage
    - Listing and filtering pending documents
    - Resolving pending documents (assign/create/discard)
    - Cleaning up expired documents
    """

    def __init__(self):
        self.db = get_supabase_client()
        self.table = "pending_documents"

    def _upload_pdf_to_storage(self, pdf_bytes: bytes, filename: str) -> str:
        """
        Upload PDF to Supabase Storage.

        Args:
            pdf_bytes: Raw PDF bytes
            filename: Original filename

        Returns:
            Storage path (e.g., "pending/uuid_filename.pdf")
        """
        # Generate unique path to avoid collisions
        unique_id = str(uuid.uuid4())[:8]
        safe_filename = filename.replace(" ", "_")
        storage_path = f"{STORAGE_PREFIX}/{unique_id}_{safe_filename}"

        logger.debug(
            "uploading_pdf_to_storage",
            storage_path=storage_path,
            size_bytes=len(pdf_bytes)
        )

        try:
            result = self.db.storage.from_(STORAGE_BUCKET).upload(
                storage_path,
                pdf_bytes,
                file_options={"content-type": "application/pdf"}
            )

            logger.info(
                "pdf_uploaded_to_storage",
                storage_path=storage_path
            )

            return storage_path

        except Exception as e:
            logger.error(
                "pdf_upload_failed",
                storage_path=storage_path,
                error=str(e)
            )
            raise DatabaseError(f"Failed to upload PDF: {e}")

    def _get_pdf_signed_url(self, storage_path: str, expires_in: int = 3600) -> str:
        """
        Get signed URL for PDF download.

        Args:
            storage_path: Path in storage bucket
            expires_in: URL expiration in seconds (default 1 hour)

        Returns:
            Signed URL for temporary access
        """
        try:
            result = self.db.storage.from_(STORAGE_BUCKET).create_signed_url(
                storage_path,
                expires_in
            )
            return result.get("signedURL", "")
        except Exception as e:
            logger.warning(
                "signed_url_failed",
                storage_path=storage_path,
                error=str(e)
            )
            return ""

    def create(
        self,
        document_type: str,
        parsed_data: ParsedDocumentData,
        pdf_bytes: bytes,
        pdf_filename: str,
        source: str = "email",
        email_subject: Optional[str] = None,
        email_from: Optional[str] = None,
        attempted_booking: Optional[str] = None,
        attempted_shp: Optional[str] = None,
        attempted_containers: Optional[list[str]] = None
    ) -> PendingDocumentResponse:
        """
        Create a new pending document record.

        Args:
            document_type: Type of document (hbl, mbl, booking, etc.)
            parsed_data: Full ParsedDocumentData from Claude
            pdf_bytes: Raw PDF file bytes
            pdf_filename: Original PDF filename
            source: Source of document ("email" or "manual")
            email_subject: Email subject if from email
            email_from: Email sender if from email
            attempted_booking: Booking number that was tried for matching
            attempted_shp: SHP number that was tried for matching
            attempted_containers: Container numbers that were tried

        Returns:
            PendingDocumentResponse with created document
        """
        logger.info(
            "creating_pending_document",
            document_type=document_type,
            source=source,
            email_from=email_from
        )

        # Upload PDF to storage first
        storage_path = self._upload_pdf_to_storage(pdf_bytes, pdf_filename)

        # Convert ParsedDocumentData to dict for JSONB storage
        parsed_dict = parsed_data.model_dump()

        # Build record
        record = {
            "document_type": document_type,
            "parsed_data": parsed_dict,
            "pdf_storage_path": storage_path,
            "source": source,
            "email_subject": email_subject,
            "email_from": email_from,
            "attempted_booking": attempted_booking,
            "attempted_shp": attempted_shp,
            "attempted_containers": attempted_containers or [],
            "status": PendingStatus.PENDING.value,
        }

        try:
            result = self.db.table(self.table).insert(record).execute()

            if not result.data:
                raise DatabaseError("No data returned from insert")

            row = result.data[0]

            logger.info(
                "pending_document_created",
                id=row["id"],
                document_type=document_type,
                storage_path=storage_path
            )

            return self._row_to_response(row)

        except Exception as e:
            logger.error(
                "pending_document_create_failed",
                error=str(e)
            )
            raise DatabaseError(f"Failed to create pending document: {e}")

    def get_by_id(self, document_id: str) -> PendingDocumentResponse:
        """
        Get a pending document by ID.

        Args:
            document_id: UUID of pending document

        Returns:
            PendingDocumentResponse

        Raises:
            NotFoundError: If document not found
        """
        logger.debug("getting_pending_document", id=document_id)

        try:
            result = self.db.table(self.table).select("*").eq("id", document_id).execute()

            if not result.data:
                raise NotFoundError(f"Pending document {document_id} not found")

            return self._row_to_response(result.data[0])

        except NotFoundError:
            raise
        except Exception as e:
            logger.error(
                "pending_document_get_failed",
                id=document_id,
                error=str(e)
            )
            raise DatabaseError(f"Failed to get pending document: {e}")

    def get_all(
        self,
        page: int = 1,
        page_size: int = 20,
        status: Optional[PendingStatus] = None,
        document_type: Optional[str] = None
    ) -> tuple[list[PendingDocumentResponse], int]:
        """
        Get all pending documents with filters.

        Args:
            page: Page number (1-indexed)
            page_size: Items per page
            status: Filter by status
            document_type: Filter by document type

        Returns:
            Tuple of (documents list, total count)
        """
        logger.debug(
            "getting_pending_documents",
            page=page,
            page_size=page_size,
            status=status,
            document_type=document_type
        )

        try:
            query = self.db.table(self.table).select("*", count="exact")

            # Apply filters
            if status:
                query = query.eq("status", status.value)
            if document_type:
                query = query.eq("document_type", document_type)

            # Apply pagination
            offset = (page - 1) * page_size
            query = query.range(offset, offset + page_size - 1)

            # Order by created_at descending (newest first)
            query = query.order("created_at", desc=True)

            result = query.execute()

            documents = [self._row_to_response(row) for row in result.data]
            total = result.count or 0

            logger.info(
                "pending_documents_retrieved",
                count=len(documents),
                total=total
            )

            return documents, total

        except Exception as e:
            logger.error(
                "pending_documents_get_all_failed",
                error=str(e)
            )
            raise DatabaseError(f"Failed to get pending documents: {e}")

    async def resolve(
        self,
        document_id: str,
        request: ResolvePendingRequest
    ) -> PendingDocumentResponse:
        """
        Resolve a pending document.

        Actions:
        - assign: Assign to existing shipment (requires target_shipment_id)
        - create: Create new shipment from this document
        - discard: Mark as discarded (not needed)

        Args:
            document_id: UUID of pending document
            request: Resolution request with action and optional shipment ID

        Returns:
            Updated PendingDocumentResponse
        """
        logger.info(
            "resolving_pending_document",
            id=document_id,
            action=request.action
        )

        # Get current document
        doc = self.get_by_id(document_id)

        if doc.status != PendingStatus.PENDING:
            raise DatabaseError(f"Document already resolved (status: {doc.status})")

        resolved_shipment_id = None
        resolved_action = None

        if request.action == "assign":
            if not request.target_shipment_id:
                raise DatabaseError("target_shipment_id required for assign action")

            # Import here to avoid circular import
            from services.shipment_service import get_shipment_service
            from routes.ingest import confirm_ingest

            # Verify shipment exists
            shipment_service = get_shipment_service()
            shipment_service.get_by_id(request.target_shipment_id)

            # Build confirm request from parsed data
            parsed = doc.parsed_data

            # Reconstruct ParsedDocumentData to pass container_details through
            original_parsed_data = None
            try:
                original_parsed_data = ParsedDocumentData(**parsed)
            except Exception as e:
                logger.warning("failed_to_reconstruct_parsed_data", error=str(e))

            confirm_req = ConfirmIngestRequest(
                shp_number=request.shp_number or (parsed.get("shp_number") or {}).get("value"),
                booking_number=request.booking_number or (parsed.get("booking_number") or {}).get("value"),
                document_type=doc.document_type,
                containers=parsed.get("containers") or [],
                vessel=(parsed.get("vessel") or {}).get("value"),
                voyage=(parsed.get("voyage") or {}).get("value"),
                pol=(parsed.get("pol") or {}).get("value"),
                pod=(parsed.get("pod") or {}).get("value"),
                etd=(parsed.get("etd") or {}).get("value"),
                eta=(parsed.get("eta") or {}).get("value"),
                atd=(parsed.get("atd") or {}).get("value"),
                ata=(parsed.get("ata") or {}).get("value"),
                source="pending_resolution",
                notes=f"Resolved from pending document queue",
                target_shipment_id=request.target_shipment_id,
                original_parsed_data=original_parsed_data,
            )

            result = await confirm_ingest(confirm_req)

            resolved_shipment_id = result.shipment_id
            resolved_action = ResolvedAction.ASSIGNED

        elif request.action == "create":
            from routes.ingest import confirm_ingest

            parsed = doc.parsed_data

            # Reconstruct ParsedDocumentData to pass container_details through
            original_parsed_data = None
            try:
                original_parsed_data = ParsedDocumentData(**parsed)
            except Exception as e:
                logger.warning("failed_to_reconstruct_parsed_data", error=str(e))

            confirm_req = ConfirmIngestRequest(
                shp_number=request.shp_number or (parsed.get("shp_number") or {}).get("value"),
                booking_number=request.booking_number or (parsed.get("booking_number") or {}).get("value"),
                document_type=doc.document_type,
                containers=parsed.get("containers") or [],
                vessel=(parsed.get("vessel") or {}).get("value"),
                voyage=(parsed.get("voyage") or {}).get("value"),
                pol=(parsed.get("pol") or {}).get("value"),
                pod=(parsed.get("pod") or {}).get("value"),
                etd=(parsed.get("etd") or {}).get("value"),
                eta=(parsed.get("eta") or {}).get("value"),
                atd=(parsed.get("atd") or {}).get("value"),
                ata=(parsed.get("ata") or {}).get("value"),
                source="pending_resolution",
                notes=f"Created from pending document queue",
                target_shipment_id=None,  # Force creation
                original_parsed_data=original_parsed_data,
            )

            result = await confirm_ingest(confirm_req)

            resolved_shipment_id = result.shipment_id
            resolved_action = ResolvedAction.CREATED

        elif request.action == "discard":
            resolved_action = ResolvedAction.DISCARDED

        # Update document status
        update_data = {
            "status": PendingStatus.RESOLVED.value,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
            "resolved_action": resolved_action.value if resolved_action else None,
            "resolved_shipment_id": resolved_shipment_id,
        }

        try:
            result = self.db.table(self.table).update(update_data).eq("id", document_id).execute()

            if not result.data:
                raise DatabaseError("No data returned from update")

            logger.info(
                "pending_document_resolved",
                id=document_id,
                action=resolved_action.value if resolved_action else None,
                shipment_id=resolved_shipment_id
            )

            return self._row_to_response(result.data[0])

        except Exception as e:
            logger.error(
                "pending_document_resolve_failed",
                id=document_id,
                error=str(e)
            )
            raise DatabaseError(f"Failed to resolve pending document: {e}")

    def expire_old_documents(self) -> int:
        """
        Mark expired pending documents.

        Called periodically to clean up old unresolved documents.

        Returns:
            Number of documents expired
        """
        logger.info("expiring_old_pending_documents")

        try:
            now = datetime.now(timezone.utc).isoformat()

            result = self.db.table(self.table).update(
                {"status": PendingStatus.EXPIRED.value}
            ).eq(
                "status", PendingStatus.PENDING.value
            ).lt(
                "expires_at", now
            ).execute()

            count = len(result.data) if result.data else 0

            logger.info("pending_documents_expired", count=count)

            return count

        except Exception as e:
            logger.error(
                "pending_documents_expire_failed",
                error=str(e)
            )
            return 0

    def _row_to_response(self, row: dict) -> PendingDocumentResponse:
        """Convert database row to response model."""
        return PendingDocumentResponse(
            id=row["id"],
            created_at=row["created_at"],
            updated_at=row.get("updated_at"),
            document_type=row["document_type"],
            parsed_data=row["parsed_data"],
            pdf_storage_path=row["pdf_storage_path"],
            source=row["source"],
            email_subject=row.get("email_subject"),
            email_from=row.get("email_from"),
            attempted_booking=row.get("attempted_booking"),
            attempted_shp=row.get("attempted_shp"),
            attempted_containers=row.get("attempted_containers") or [],
            status=PendingStatus(row["status"]),
            resolved_at=row.get("resolved_at"),
            resolved_shipment_id=row.get("resolved_shipment_id"),
            resolved_action=ResolvedAction(row["resolved_action"]) if row.get("resolved_action") else None,
            expires_at=row["expires_at"],
        )


# Singleton instance
_pending_document_service: Optional[PendingDocumentService] = None


def get_pending_document_service() -> PendingDocumentService:
    """Get or create PendingDocumentService instance."""
    global _pending_document_service
    if _pending_document_service is None:
        _pending_document_service = PendingDocumentService()
    return _pending_document_service
