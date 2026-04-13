"""User management routes (admin only — protected by JWT middleware).

Uses Supabase admin client (service role key) to create/list/delete users.
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from config import get_admin_client


router = APIRouter(prefix="/api/users", tags=["Users"])


class CreateUserRequest(BaseModel):
    email: str = Field(min_length=3, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    password: str = Field(min_length=8, max_length=72)


class UserOut(BaseModel):
    id: str
    email: str | None
    created_at: str | None
    last_sign_in_at: str | None


def _admin():
    client = get_admin_client()
    if client is None:
        raise HTTPException(
            status_code=500,
            detail="Server misconfigured: SUPABASE_SERVICE_KEY not set",
        )
    return client


@router.get("", response_model=list[UserOut])
async def list_users():
    """List all users in the project."""
    client = _admin()
    resp = client.auth.admin.list_users()
    users = resp if isinstance(resp, list) else getattr(resp, "users", [])
    return [
        UserOut(
            id=str(u.id),
            email=u.email,
            created_at=str(u.created_at) if u.created_at else None,
            last_sign_in_at=str(u.last_sign_in_at) if u.last_sign_in_at else None,
        )
        for u in users
    ]


@router.post("", response_model=UserOut, status_code=201)
async def create_user(body: CreateUserRequest):
    """Create a new user (auto-confirmed, no email verification needed)."""
    client = _admin()
    try:
        resp = client.auth.admin.create_user({
            "email": body.email,
            "password": body.password,
            "email_confirm": True,
        })
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    user = getattr(resp, "user", None) or resp
    return UserOut(
        id=str(user.id),
        email=user.email,
        created_at=str(user.created_at) if user.created_at else None,
        last_sign_in_at=None,
    )


@router.delete("/{user_id}", status_code=204)
async def delete_user(user_id: str, request: Request):
    """Delete a user. Cannot delete yourself."""
    current_user_id = getattr(request.state, "user_id", None)
    if current_user_id == user_id:
        raise HTTPException(
            status_code=400, detail="You cannot delete your own account"
        )
    client = _admin()
    try:
        client.auth.admin.delete_user(user_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return None
