from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from notify.email_notifier import EmailConfig, EmailNotifier


class EmailNotifierTest(unittest.TestCase):
    def test_config_from_env(self) -> None:
        env = {
            "EMAIL_SMTP_HOST": "smtp.163.com",
            "EMAIL_SMTP_PORT": "465",
            "EMAIL_SMTP_USERNAME": "sender@example.com",
            "EMAIL_SMTP_PASSWORD": "secret",
            "EMAIL_FROM": "sender@example.com",
            "EMAIL_TO": "a@example.com;b@example.com",
        }
        with patch.dict(os.environ, env, clear=True):
            config = EmailConfig.from_env()

        self.assertEqual(config.smtp_host, "smtp.163.com")
        self.assertEqual(config.smtp_port, 465)
        self.assertEqual(config.username, "sender@example.com")
        self.assertEqual(config.sender, "sender@example.com")
        self.assertEqual(config.recipients, ("a@example.com", "b@example.com"))
        self.assertTrue(config.use_ssl)

    def test_config_requires_minimum_env(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "EMAIL_SMTP_HOST"):
                EmailConfig.from_env()

    @patch("notify.email_notifier.smtplib.SMTP_SSL")
    def test_send_uses_smtp_ssl(self, smtp_ssl: MagicMock) -> None:
        smtp = smtp_ssl.return_value.__enter__.return_value
        config = EmailConfig(
            smtp_host="smtp.163.com",
            smtp_port=465,
            username="sender@example.com",
            password="secret",
            sender="sender@example.com",
            recipients=("receiver@example.com",),
        )

        result = EmailNotifier(config=config).send(
            "info",
            "Test title",
            "Test content",
            {"subject_prefix": "[Test]"},
        )

        self.assertTrue(result)
        smtp.login.assert_called_once_with("sender@example.com", "secret")
        smtp.send_message.assert_called_once()
        message = smtp.send_message.call_args.args[0]
        self.assertEqual(message["To"], "receiver@example.com")
        self.assertEqual(message["Subject"], "[Test] [INFO] Test title")

    def test_build_message_supports_utf8_and_attachments(self) -> None:
        config = EmailConfig(
            smtp_host="smtp.163.com",
            smtp_port=465,
            username="sender@example.com",
            password="secret",
            sender="sender@example.com",
            recipients=("receiver@example.com",),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            image_path = Path(tmpdir) / "chart.png"
            image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

            message = EmailNotifier._build_message(
                config,
                "info",
                "\u4e2d\u6587\u6807\u9898",
                "\u4e2d\u6587\u6b63\u6587",
                {"attachments": [image_path]},
            )

        self.assertTrue(message.is_multipart())
        text_part = next(part for part in message.walk() if part.get_content_type() == "text/plain")
        image_part = next(part for part in message.walk() if part.get_content_type() == "image/png")
        self.assertEqual(text_part.get_content_charset(), "utf-8")
        self.assertEqual(text_part["Content-Transfer-Encoding"], "base64")
        self.assertEqual(image_part.get_filename(), "chart.png")

    def test_build_message_supports_inline_images(self) -> None:
        config = EmailConfig(
            smtp_host="smtp.163.com",
            smtp_port=465,
            username="sender@example.com",
            password="secret",
            sender="sender@example.com",
            recipients=("receiver@example.com",),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            image_path = Path(tmpdir) / "chart.png"
            image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

            message = EmailNotifier._build_message(
                config,
                "info",
                "HTML report",
                "plain fallback",
                {
                    "html": '<html><body><img src="cid:chart_main"></body></html>',
                    "inline_images": [{"path": image_path, "cid": "chart_main"}],
                },
            )

        inline_part = next(part for part in message.walk() if part.get_content_type() == "image/png")
        self.assertEqual(inline_part["Content-ID"], "<chart_main>")
        self.assertEqual(inline_part.get_filename(), "chart.png")


if __name__ == "__main__":
    unittest.main()
