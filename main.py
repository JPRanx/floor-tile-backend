"""
Floor Tile SaaS — Main Application

FastAPI application entry point.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import structlog
from datetime import datetime

from config import settings, check_connection

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer() if settings.is_production 
            else structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    
    Startup: Check database connection
    Shutdown: Clean up resources
    """
    # Startup
    logger.info(
        "application_starting",
        environment=settings.environment,
        debug=settings.debug
    )
    
    # Check database connection
    db_status = check_connection()
    if db_status["status"] == "healthy":
        logger.info(
            "database_connected",
            products=db_status["products_count"],
            settings=db_status["settings_count"]
        )
    else:
        logger.error(
            "database_connection_failed",
            error=db_status.get("error")
        )
    
    yield
    
    # Shutdown
    logger.info("application_shutting_down")


# Create FastAPI app
app = FastAPI(
    title="Floor Tile SaaS",
    description="Inventory, logistics, and order optimization for floor tile distribution",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs" if settings.debug else None,
    redoc_url="/redoc" if settings.debug else None,
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
        "http://localhost:5176",
        "http://localhost:5177",
        "http://localhost:5178",
        "https://floor-tile-frontend.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===================
# ROUTES
# ===================

@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Basic health status and database connection state
    """
    db_status = check_connection()
    
    return {
        "status": "healthy" if db_status["status"] == "healthy" else "degraded",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": settings.environment,
        "database": db_status
    }


@app.get("/")
async def root():
    """
    Root endpoint.
    
    Returns:
        API information and available endpoints
    """
    return {
        "name": "Floor Tile SaaS API",
        "version": "0.1.0",
        "docs": "/docs" if settings.debug else "Disabled in production",
        "health": "/health",
        "endpoints": {
            "products": "/api/products",
            "inventory": "/api/inventory",
            "sales": "/api/sales",
            "factory_orders": "/api/factory-orders",
            "shipments": "/api/shipments",
            "containers": "/api/containers",
            "dashboard": "/api/dashboard",
            "recommendations": "/api/recommendations",
            "order_builder": "/api/order-builder",
            "boats": "/api/boats",
            "alerts": "/api/alerts",
            "settings": "/api/settings"
        }
    }


# ===================
# ERROR HANDLERS
# ===================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler.
    
    Catches unhandled exceptions and returns standard error format.
    See STANDARDS_ERRORS.md for error response format.
    """
    logger.error(
        "unhandled_exception",
        path=request.url.path,
        method=request.method,
        error=str(exc),
        error_type=type(exc).__name__
    )
    
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "An unexpected error occurred",
                "details": str(exc) if settings.debug else None,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
    )


# ===================
# INCLUDE ROUTERS
# ===================
from routes.products import router as products_router
from routes.inventory import router as inventory_router
from routes.sales import router as sales_router
from routes.dashboard import router as dashboard_router
from routes.settings import router as settings_router
from routes.recommendations import router as recommendations_router
from routes.boats import router as boats_router
from routes.order_builder import router as order_builder_router
from routes.factory_orders import router as factory_orders_router
from routes.shipments import router as shipments_router

app.include_router(products_router, prefix="/api/products", tags=["Products"])
app.include_router(inventory_router, prefix="/api/inventory", tags=["Inventory"])
app.include_router(sales_router, prefix="/api/sales", tags=["Sales"])
app.include_router(dashboard_router, prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(settings_router, prefix="/api/settings", tags=["Settings"])
app.include_router(recommendations_router, prefix="/api/recommendations", tags=["Recommendations"])
app.include_router(boats_router, prefix="/api/boats", tags=["Boats"])
app.include_router(order_builder_router)  # Prefix already in router
app.include_router(factory_orders_router)  # Prefix already in router
app.include_router(shipments_router)  # Prefix already in router


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )
