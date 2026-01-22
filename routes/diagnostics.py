"""
Data Quality Diagnostics API routes.

Provides endpoint for comprehensive data quality analysis.
"""

from fastapi import APIRouter
import structlog

from services.diagnostic_service import get_diagnostic_service

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/data-quality")
async def get_data_quality_report():
    """
    Run comprehensive data quality diagnostics.

    Returns a detailed report with:
    - 15 different quality checks
    - Status for each (pass/warning/fail)
    - Explanations of expected vs unexpected findings
    - Actionable items for data issues

    This endpoint helps identify:
    - Data anomalies that look like bugs but are expected
    - Actual bugs that need fixing
    - Data gaps that need attention
    """
    logger.info("running_data_quality_diagnostics")

    service = get_diagnostic_service()
    report = service.run_all_checks()

    logger.info(
        "diagnostics_complete",
        passed=report["summary"]["passed"],
        warnings=report["summary"]["warnings"],
        failures=report["summary"]["failures"]
    )

    return report


@router.get("/health")
async def health_check():
    """Simple health check for the diagnostics service."""
    return {
        "status": "healthy",
        "service": "diagnostics"
    }
