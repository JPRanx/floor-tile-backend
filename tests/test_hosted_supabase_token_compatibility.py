from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric import ec
import jwt

from clean_v1.persistence.identity_supabase import IdentityBinding, SupabaseIdentityResolver


class _Bindings:
    def __init__(self, subject: str) -> None:
        self.subject = subject

    def find_active_by_auth_user_id(self, auth_user_id: str):
        if auth_user_id != self.subject:
            return None
        return IdentityBinding(
            auth_user_id=auth_user_id,
            actor="ashley",
            company_id="22222222-2222-4222-8222-222222222222",
            role="administrator",
            active=True,
        )


def test_hosted_resolver_accepts_supabase_es256_token_without_optional_nbf():
    subject = "11111111-1111-4111-8111-111111111111"
    project_ref = "sqxmnhiiesfhidsgyuiw"
    key = ec.generate_private_key(ec.SECP256R1())
    public_jwk = jwt.PyJWK.from_dict(
        {
            **json_web_key(key.public_key()),
            "kid": "supabase-es256",
            "alg": "ES256",
            "use": "sig",
        }
    )
    token = jwt.encode(
        {
            "iss": f"https://{project_ref}.supabase.co/auth/v1",
            "aud": "authenticated",
            "sub": subject,
            "exp": 4102444800,
        },
        key,
        algorithm="ES256",
        headers={"kid": "supabase-es256"},
    )
    resolver = SupabaseIdentityResolver(
        project_ref=project_ref,
        audience="authenticated",
        jwks={"keys": [public_jwk._jwk_data]},
        bindings=_Bindings(subject),
        allowed_algorithms=("RS256", "ES256"),
    )

    principal = resolver.resolve_principal(token)

    assert principal.actor == "ashley"
    assert principal.company_id == "22222222-2222-4222-8222-222222222222"
    assert principal.role == "administrator"


def test_default_resolver_accepts_supabase_es256_algorithm():
    subject = "11111111-1111-4111-8111-111111111111"
    project_ref = "sqxmnhiiesfhidsgyuiw"
    key = ec.generate_private_key(ec.SECP256R1())
    jwk = {
        **json_web_key(key.public_key()),
        "kid": "supabase-es256-default",
        "alg": "ES256",
        "use": "sig",
    }
    token = jwt.encode(
        {
            "iss": f"https://{project_ref}.supabase.co/auth/v1",
            "aud": "authenticated",
            "sub": subject,
            "exp": 4102444800,
            "nbf": 0,
        },
        key,
        algorithm="ES256",
        headers={"kid": "supabase-es256-default"},
    )
    resolver = SupabaseIdentityResolver(
        project_ref=project_ref,
        audience="authenticated",
        jwks={"keys": [jwk]},
        bindings=_Bindings(subject),
    )

    principal = resolver.resolve_principal(token)

    assert principal.actor == "ashley"


def json_web_key(public_key):
    numbers = public_key.public_numbers()
    encode = lambda n: __import__("base64").urlsafe_b64encode(
        n.to_bytes(32, "big")
    ).rstrip(b"=").decode()
    return {
        "kty": "EC",
        "crv": "P-256",
        "x": encode(numbers.x),
        "y": encode(numbers.y),
    }
