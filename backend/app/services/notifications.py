from __future__ import annotations

import re

import httpx

from backend.app.config import Settings, get_settings
from backend.app.domain import Recommendation

SECRET_PATTERN = re.compile(r"(?i)(api[_-]?key|token|secret)\s*[:=]\s*\S+")


class TelegramNotifier:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = httpx.Client(timeout=15)

    @property
    def enabled(self) -> bool:
        return bool(self.settings.telegram_bot_token and self.settings.telegram_chat_id)

    def send(self, text: str) -> bool:
        if not self.enabled:
            return False
        sanitized = SECRET_PATTERN.sub("[REDACTED]", text)
        sanitized = sanitized.replace(self.settings.telegram_bot_token, "[REDACTED]")[:4000]
        try:
            response = self.client.post(
                f"https://api.telegram.org/bot{self.settings.telegram_bot_token}/sendMessage",
                json={
                    "chat_id": self.settings.telegram_chat_id,
                    "text": sanitized,
                    "disable_web_page_preview": True,
                },
            )
            return response.is_success
        except httpx.HTTPError:
            return False

    def recommendation(self, value: Recommendation) -> bool:
        return self.send(
            f"研究完成：{value.asset.name} ({value.asset.symbol})\n"
            f"状态：{value.signal_status.value}｜评级：{value.rating.value}"
            f"｜程序分：{value.score}｜置信度：{value.confidence:.0%}\n"
            f"资料覆盖：{'完整' if value.evidence_complete else '不足'}"
            f"｜方向门禁：{'通过' if value.directional_evidence_complete else '未通过'}\n"
            f"{value.thesis.summary[:800]}\n\n仅用于研究与模拟。"
        )


notifier = TelegramNotifier()
