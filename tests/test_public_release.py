import unittest
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PublicReleaseTests(unittest.TestCase):
    def test_public_name_and_defaults(self):
        content = "\n".join(
            (ROOT / name).read_text(encoding="utf-8")
            for name in ("app_settings.py", "config.py", "bot.py", ".env.example")
        )
        self.assertIn("Account Sales Bot", content)
        self.assertNotIn("digiacc", content.lower())
        self.assertNotIn("dgbot", content.lower())
        self.assertNotIn("yourbrand", content.lower())

    def test_installer_is_direct_ubuntu_systemd_install(self):
        installer = (ROOT / "install.sh").read_text(encoding="utf-8")
        self.assertIn("Ubuntu 22.04", installer)
        self.assertIn("python3-venv", installer)
        self.assertIn("account-sales-bot.service", installer)
        self.assertIn("systemctl", installer)
        self.assertIn("/etc/account-sales-bot", installer)
        self.assertIn("/var/lib/account-sales-bot", installer)
        self.assertIn("https://github.com/peyley95/account-sales-bot.git", installer)
        self.assertIn('chmod 0600 "$ENV_FILE"', installer)
        self.assertIn("Enter BOT_TOKEN:", installer)
        self.assertIn("Enter numeric Telegram ID for the root admin:", installer)
        self.assertNotIn("read -r -s", installer)
        self.assertIsNone(re.search(r"[\u0600-\u06FF]", installer))
        self.assertNotIn("docker", installer.lower())
        self.assertNotIn("set -x", installer)

    def test_all_shell_output_is_english_only(self):
        shell_files = [ROOT / "install.sh", ROOT / "update.sh", ROOT / "scripts/validate.sh"]
        for path in shell_files:
            content = path.read_text(encoding="utf-8")
            self.assertIsNone(re.search(r"[\u0600-\u06FF]", content), path.name)
        self.assertIn("Starting Account Sales Bot update", shell_files[1].read_text(encoding="utf-8"))

    def test_container_artifacts_are_not_published(self):
        for relative in (
            "Dockerfile", "compose.yaml", "entrypoint.sh", ".dockerignore",
            "routeros-install.rsc.template",
        ):
            self.assertFalse((ROOT / relative).exists(), relative)

    def test_sensitive_runtime_artifacts_are_gitignored(self):
        ignored = (ROOT / ".gitignore").read_text(encoding="utf-8")
        for pattern in (".env", ".venv/", "data/", "*.sqlite3", "*.db", "*.zip"):
            self.assertIn(pattern, ignored)

    def test_public_support_files_exist(self):
        required = (
            "README.md", "LICENSE", ".env.example", "install.sh", "update.sh",
            ".github/workflows/ci.yml", "restore_manager.py",
        )
        for relative in required:
            self.assertTrue((ROOT / relative).is_file(), relative)

    def test_internal_release_artifacts_are_not_published(self):
        for relative in (
            "admin_config_ui.py", "PUBLIC_RELEASE_CHECKLIST.md",
            "ENV-PLANS-GUIDE.txt", "CONTRIBUTING.md", "SECURITY.md",
            "CHANGELOG.md", ".github/dependabot.yml",
        ):
            self.assertFalse((ROOT / relative).exists(), relative)

    def test_readme_contains_codex_attribution(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("صفر تا صد این ربات", readme)
        self.assertIn("Codex", readme)
        self.assertIn("ChatGPT", readme)
        self.assertIn("sudo apt update", readme)
        self.assertIn("sudo apt install -y curl ca-certificates", readme)
        self.assertIn("https://raw.githubusercontent.com/peyley95/account-sales-bot/main/install.sh", readme)
        self.assertIn("sudo systemctl stop account-sales-bot", readme)
        self.assertIn("sudo systemctl start account-sales-bot", readme)
        self.assertIn("sudo systemctl disable --now account-sales-bot", readme)
        self.assertIn("sudo rm -rf /var/lib/account-sales-bot", readme)
        self.assertLess(len(readme), 5000)

    def test_example_env_is_bootstrap_only(self):
        lines = {
            line.split("=", 1)[0]
            for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#") and "=" in line
        }
        self.assertEqual(lines, {"BOT_TOKEN", "ADMIN_IDS", "DATA_DIR"})

    def test_version_is_current_public_release(self):
        self.assertEqual((ROOT / "VERSION").read_text(encoding="utf-8").strip(), "1.2.0")


if __name__ == "__main__":
    unittest.main()
