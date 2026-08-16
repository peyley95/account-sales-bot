<h1 align="center">Account Sales Bot</h1>

<div dir="rtl" align="right">

یک ربات سبک تلگرام نوشته‌شده با Python که به User Manager میکروتیک و پنل ثنایی (3x-ui نسخه 3 به بالا) متصل می‌شود و ساخت، فروش و تمدید اکانت را به‌صورت خودکار انجام می‌دهد.

## قابلیت‌ها

- فروش و تمدید اکانت‌های MikroTik و ثنایی
- فروش OpenVPN، V2Ray یا هر دو سرویس به‌صورت هم‌زمان
- مدیریت بسته‌های مستقل و اکانت تست
- افزودن ریسلر و تعیین قیمت اختصاصی بر اساس هر گیگ فروش
- ثبت و مدیریت بدهی ریسلرها
- کیف پول کاربران با امکان فعال یا غیرفعال‌سازی
- سیستم Referral و بازاریابی با امکان فعال یا غیرفعال‌سازی
- پرداخت کارت‌به‌کارت و زرین‌پال
- مدیریت کامل تنظیمات از پنل ادمین تلگرام
- بکاپ خودکار و نگهداری اطلاعات در SQLite

## نصب روی Ubuntu 22.04 یا جدیدتر

ابتدا پیش‌نیازهای نصب را آماده کنید:

<div dir="ltr" align="left">

```bash
sudo apt update
sudo apt install -y curl ca-certificates
```

</div>

سپس نصب‌کننده را اجرا کنید:

<div dir="ltr" align="left">

```bash
curl -fsSL https://raw.githubusercontent.com/peyley95/account-sales-bot/main/install.sh | sudo bash
```

</div>

نصب‌کننده توکن ربات و Telegram ID عددی مدیر اصلی را دریافت می‌کند و سرویس را با systemd راه‌اندازی می‌کند. بعد از نصب، در تلگرام `/start` را بزنید و اتصال سرویس‌ها، درگاه‌های پرداخت و بسته‌ها را از پنل ادمین تنظیم کنید.

## به‌روزرسانی

<div dir="ltr" align="left">

```bash
sudo bash /opt/account-sales-bot/update.sh
```

</div>

هیچ‌وقت توکن، رمزها، فایل ENV یا دیتابیس واقعی خود را در GitHub منتشر نکنید.

## درباره پروژه

صفر تا صد این ربات، از طراحی و کدنویسی تا تست و مستندسازی، با Codex در ChatGPT ساخته شده است.

این پروژه با مجوز [MIT](LICENSE) منتشر می‌شود.

## توقف و اجرای مجدد ربات

<div dir="ltr" align="left">

```bash
sudo systemctl stop account-sales-bot
sudo systemctl start account-sales-bot
sudo systemctl restart account-sales-bot
sudo systemctl status account-sales-bot --no-pager
sudo journalctl -u account-sales-bot -f
```

</div>

## حذف کامل ربات از سرور

هشدار: دستورهای زیر سرویس، سورس، تنظیمات، دیتابیس و تمام بکاپ‌های ربات را برای همیشه حذف می‌کنند. قبل از اجرا، در صورت نیاز از مسیر `/var/lib/account-sales-bot` بکاپ بگیرید.

<div dir="ltr" align="left">

```bash
sudo systemctl disable --now account-sales-bot
sudo rm -f /etc/systemd/system/account-sales-bot.service
sudo systemctl daemon-reload
sudo systemctl reset-failed
sudo userdel accountbot 2>/dev/null || true
sudo rm -rf /opt/account-sales-bot
sudo rm -rf /etc/account-sales-bot
sudo rm -rf /var/lib/account-sales-bot
```

</div>

</div>
