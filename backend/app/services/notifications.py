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
        news_confidence = value.news_confidence
        if news_confidence is None:
            news_confidence = (
                value.fact_confidence
                if value.fact_confidence is not None
                else value.evidence_strength
            )
        rating_confidence = (
            value.rating_confidence
            if value.rating_confidence is not None
            else value.confidence
        )
        direction_score = (
            value.direction_score if value.direction_score is not None else value.score
        )
        return self.send(
            f"研究完成：{value.asset.name} ({value.asset.symbol})\n"
            f"状态：{value.signal_status.value}｜评级：{value.rating.value}"
            f"｜方向分：{direction_score}｜评级置信度：{rating_confidence:.0%}\n"
            f"新闻可信度：{news_confidence:.0%}"
            f"｜未来 {value.horizon_days} 个自然日"
            f"｜证据质量提示：{len(value.evidence_warnings)} 条\n"
            f"{value.thesis.summary[:800]}\n\n仅用于研究与模拟。"
        )


notifier = TelegramNotifier()
