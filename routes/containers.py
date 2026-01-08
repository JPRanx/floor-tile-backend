"""
Container API Routes - CRUD operations for containers and container items.

Endpoints for managing containers within shipments.
"""

from fastapi import APIRouter, HTTPException, status
from typing import List

from services.container_service import get_container_service
from models.container import (
    ContainerCreate,
    ContainerUpdate,
    ContainerResponse,
    ContainerItemCreate,
    ContainerItemUpdate,
    ContainerItemResponse,
    ContainerWithItemsResponse,
    ContainerListResponse
)
from exceptions import (
    ContainerNotFoundError,
    ContainerItemNotFoundError,
    ShipmentNotFoundError,
    ProductNotFoundError,
    DatabaseError
)

router = APIRouter(prefix="/api", tags=["containers"])


# ===================
# CONTAINER ENDPOINTS
# ===================

@router.get(
    "/shipments/{shipment_id}/containers",
    response_model=ContainerListResponse,
    summary="List all containers for a shipment"
)
def list_containers_by_shipment(shipment_id: str):
    """
    Get all containers for a specific shipment.

    Args:
        shipment_id: Shipment UUID

    Returns:
        List of containers with their totals
    """
    service = get_container_service()
    try:
        containers = service.get_by_shipment(shipment_id)
        return ContainerListResponse(data=containers, total=len(containers))
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.get(
    "/containers/{container_id}",
    response_model=ContainerWithItemsResponse,
    summary="Get container with items"
)
def get_container(container_id: str):
    """
    Get a container by ID with all its items.

    Args:
        container_id: Container UUID

    Returns:
        Container with all items
    """
    service = get_container_service()
    try:
        return service.get_with_items(container_id)
    except ContainerNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.post(
    "/shipments/{shipment_id}/containers",
    response_model=ContainerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new container"
)
def create_container(shipment_id: str, data: ContainerCreate):
    """
    Create a new container for a shipment.

    Args:
        shipment_id: Shipment UUID
        data: Container creation data

    Returns:
        Created container
    """
    service = get_container_service()

    # Ensure shipment_id matches
    if data.shipment_id != shipment_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": {"code": "SHIPMENT_ID_MISMATCH", "message": "Shipment ID in URL must match request body"}}
        )

    try:
        return service.create(data)
    except ShipmentNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.patch(
    "/containers/{container_id}",
    response_model=ContainerResponse,
    summary="Update container"
)
def update_container(container_id: str, data: ContainerUpdate):
    """
    Update container fields.

    Args:
        container_id: Container UUID
        data: Fields to update

    Returns:
        Updated container
    """
    service = get_container_service()
    try:
        return service.update(container_id, data)
    except ContainerNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.delete(
    "/containers/{container_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete container"
)
def delete_container(container_id: str):
    """
    Delete a container (CASCADE deletes all items).

    Args:
        container_id: Container UUID
    """
    service = get_container_service()
    try:
        service.delete(container_id)
    except ContainerNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


# ===================
# CONTAINER ITEM ENDPOINTS
# ===================

@router.post(
    "/containers/{container_id}/items",
    response_model=ContainerItemResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add item to container"
)
def add_container_item(container_id: str, data: ContainerItemCreate):
    """
    Add a product item to a container.

    Args:
        container_id: Container UUID
        data: Item data (product_id, quantity, pallets, weight)

    Returns:
        Created container item
    """
    service = get_container_service()
    try:
        return service.add_item(container_id, data)
    except ContainerNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except ProductNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.patch(
    "/containers/{container_id}/items/{item_id}",
    response_model=ContainerItemResponse,
    summary="Update container item"
)
def update_container_item(container_id: str, item_id: str, data: ContainerItemUpdate):
    """
    Update a container item.

    Args:
        container_id: Container UUID (for URL consistency)
        item_id: Container item UUID
        data: Fields to update

    Returns:
        Updated container item
    """
    service = get_container_service()
    try:
        # Verify item belongs to container
        item = service.get_item(item_id)
        if item.container_id != container_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": {"code": "ITEM_CONTAINER_MISMATCH", "message": "Item does not belong to this container"}}
            )

        return service.update_item(item_id, data)
    except ContainerItemNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.delete(
    "/containers/{container_id}/items/{item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete container item"
)
def delete_container_item(container_id: str, item_id: str):
    """
    Delete an item from a container.

    Args:
        container_id: Container UUID (for URL consistency)
        item_id: Container item UUID
    """
    service = get_container_service()
    try:
        # Verify item belongs to container
        item = service.get_item(item_id)
        if item.container_id != container_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": {"code": "ITEM_CONTAINER_MISMATCH", "message": "Item does not belong to this container"}}
            )

        service.delete_item(item_id)
    except ContainerItemNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


# ===================
# UTILITY ENDPOINTS
# ===================

@router.post(
    "/containers/{container_id}/recalculate",
    response_model=ContainerResponse,
    summary="Recalculate container totals"
)
def recalculate_container_totals(container_id: str):
    """
    Recalculate container totals from items.

    Sums up all item quantities, pallets, and weights.

    Args:
        container_id: Container UUID

    Returns:
        Updated container with recalculated totals
    """
    service = get_container_service()
    try:
        return service.recalculate_totals(container_id)
    except ContainerNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )


@router.get(
    "/containers/{container_id}/validate",
    summary="Validate container limits"
)
def validate_container_limits(container_id: str):
    """
    Validate container against soft limits.

    Checks:
    - Weight <= 28,000 kg
    - Pallets <= 14
    - Area <= 1,881 m²

    Args:
        container_id: Container UUID

    Returns:
        Validation result with warnings if limits exceeded
    """
    service = get_container_service()
    try:
        is_valid, warnings = service.validate_limits(container_id)
        return {
            "container_id": container_id,
            "is_valid": is_valid,
            "warnings": warnings
        }
    except ContainerNotFoundError as e:
        raise HTTPException(status_code=e.status_code, detail=e.to_dict())
    except DatabaseError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=e.to_dict()
        )