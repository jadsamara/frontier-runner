from __future__ import annotations

from frontier.config import ConfigError

DOCS_ORIGIN = "https://frontier-web-x3l3etwczq-pd.a.run.app"


class InstallError(ConfigError):
    """Installation or onboarding failure with a stable reason code."""

    def __init__(
        self,
        code: str,
        explanation: str,
        *,
        cause: str,
        next_action: str,
        docs_path: str = "/docs/troubleshooting",
    ) -> None:
        self.code = code
        self.explanation = explanation
        self.cause = cause
        self.next_action = next_action
        self.docs_url = f"{DOCS_ORIGIN}{docs_path}"
        super().__init__(self.format())

    def format(self) -> str:
        return (
            f"{self.code}\n"
            f"{self.explanation}\n"
            f"Likely cause: {self.cause}\n"
            f"Next: {self.next_action}\n"
            f"Docs: {self.docs_url}"
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "explanation": self.explanation,
            "cause": self.cause,
            "nextAction": self.next_action,
            "docsUrl": self.docs_url,
        }
