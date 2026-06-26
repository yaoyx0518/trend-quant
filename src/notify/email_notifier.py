from __future__ import annotations

import os
import smtplib
import ssl
import mimetypes
from dataclasses import dataclass
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path

from audit.app_logger import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class EmailConfig:
    smtp_host: str
    smtp_port: int
    username: str
    password: str
    sender: str
    recipients: tuple[str, ...]
    use_ssl: bool = True

    @classmethod
    def from_env(cls) -> "EmailConfig":
        host = str(os.getenv("EMAIL_SMTP_HOST", "") or "").strip()
        port_raw = str(os.getenv("EMAIL_SMTP_PORT", "465") or "465").strip()
        username = str(os.getenv("EMAIL_SMTP_USERNAME", "") or "").strip()
        password = str(os.getenv("EMAIL_SMTP_PASSWORD", "") or "")
        sender = str(os.getenv("EMAIL_FROM", "") or username).strip()
        recipients_raw = str(os.getenv("EMAIL_TO", "") or "").strip()
        recipients = tuple(
            item.strip() for item in recipients_raw.replace(";", ",").split(",") if item.strip()
        )

        missing = []
        if not host:
            missing.append("EMAIL_SMTP_HOST")
        if not username:
            missing.append("EMAIL_SMTP_USERNAME")
        if not password:
            missing.append("EMAIL_SMTP_PASSWORD")
        if not sender:
            missing.append("EMAIL_FROM")
        if not recipients:
            missing.append("EMAIL_TO")
        if missing:
            raise ValueError(f"Missing email environment variables: {', '.join(missing)}")

        try:
            port = int(port_raw)
        except ValueError as exc:
            raise ValueError("EMAIL_SMTP_PORT must be an integer") from exc

        use_ssl = str(os.getenv("EMAIL_SMTP_USE_SSL", "true")).strip().lower()
        return cls(
            smtp_host=host,
            smtp_port=port,
            username=username,
            password=password,
            sender=sender,
            recipients=recipients,
            use_ssl=use_ssl not in {"0", "false", "no"},
        )


class EmailNotifier:
    channel = "email"

    def __init__(self, config: EmailConfig | None = None) -> None:
        self.config = config

    def send(self, level: str, title: str, content: str, context: dict | None = None) -> bool:
        config = self.config or EmailConfig.from_env()
        message = self._build_message(config, level, title, content, context or {})

        try:
            if config.use_ssl:
                ssl_context = ssl.create_default_context()
                with smtplib.SMTP_SSL(
                    config.smtp_host,
                    config.smtp_port,
                    context=ssl_context,
                    timeout=20,
                ) as smtp:
                    smtp.login(config.username, config.password)
                    smtp.send_message(message)
            else:
                with smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=20) as smtp:
                    smtp.starttls(context=ssl.create_default_context())
                    smtp.login(config.username, config.password)
                    smtp.send_message(message)
        except Exception:
            logger.exception(
                "Email notification failed: host=%s port=%s sender=%s recipients=%s",
                config.smtp_host,
                config.smtp_port,
                config.sender,
                list(config.recipients),
            )
            return False

        logger.info(
            "Email notification sent: level=%s title=%s recipients=%s",
            level,
            title,
            list(config.recipients),
        )
        return True

    @staticmethod
    def _build_message(
        config: EmailConfig,
        level: str,
        title: str,
        content: str,
        context: dict,
    ) -> EmailMessage:
        subject_prefix = str(context.get("subject_prefix", "[Trend ETF]") or "").strip()
        subject_level = str(level or "INFO").strip().upper()
        subject = f"{subject_prefix} [{subject_level}] {title}".strip()

        message = EmailMessage()
        message["From"] = config.sender
        message["To"] = ", ".join(config.recipients)
        message["Date"] = formatdate(localtime=True)
        message["Message-ID"] = make_msgid(domain=config.sender.split("@")[-1])
        message["Subject"] = subject

        html = context.get("html")
        html_part = None
        if html:
            message.set_content(content, charset="utf-8", cte="base64")
            message.add_alternative(str(html), subtype="html", charset="utf-8", cte="base64")
            html_part = message.get_payload()[-1]
        else:
            message.set_content(content, charset="utf-8", cte="base64")

        if html_part is not None:
            for inline_image in context.get("inline_images", []) or []:
                EmailNotifier._attach_inline_image(html_part, inline_image)

        for attachment in context.get("attachments", []) or []:
            EmailNotifier._attach_file(message, attachment)
        return message

    @staticmethod
    def _attach_inline_image(html_part: EmailMessage, inline_image: str | Path | dict) -> None:
        if isinstance(inline_image, dict):
            path = Path(str(inline_image["path"]))
            cid = str(inline_image.get("cid") or path.stem)
            filename = str(inline_image.get("filename") or path.name)
            content_type = str(
                inline_image.get("content_type")
                or mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            )
        else:
            path = Path(inline_image)
            cid = path.stem
            filename = path.name
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        maintype, subtype = content_type.split("/", 1)
        html_part.add_related(
            path.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            cid=f"<{cid}>",
            filename=filename,
        )

    @staticmethod
    def _attach_file(message: EmailMessage, attachment: str | Path | dict) -> None:
        if isinstance(attachment, dict):
            path = Path(str(attachment["path"]))
            filename = str(attachment.get("filename") or path.name)
            content_type = str(
                attachment.get("content_type")
                or mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            )
        else:
            path = Path(attachment)
            filename = path.name
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        maintype, subtype = content_type.split("/", 1)
        message.add_attachment(
            path.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=filename,
        )
