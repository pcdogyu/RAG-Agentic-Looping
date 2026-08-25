from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken

from backend.app.config import Settings, get_settings


def _fernet(settings: Settings) -> Fernet:
    if not settings.mcp_secret_key:
        raise RuntimeError("MCP_SECRET_KEY is not configured")
    try:
        return Fernet(settings.mcp_secret_key.encode())
    except (ValueError, TypeError) as exc:
        raise RuntimeError("MCP_SECRET_KEY is not a valid Fernet key") from exc


def encrypt_secret(secret: str, settings: Settings | None = None) -> str:
    return _fernet(settings or get_settings()).encrypt(secret.encode()).decode()


def decrypt_secret(ciphertext: str, settings: Settings | None = None) -> str:
    try:
        return _fernet(settings or get_settings()).decrypt(ciphertext.encode()).decode()
    except InvalidToken as exc:
        raise RuntimeError("stored credential cannot be decrypted") from exc
