<h1 align="center">Account Sales Bot</h1>

<div dir="rtl" align="right">

یک ربات سبک تلگرام نوشته‌شده با Python که به User Manager میکروتیک و پنل ثنایی (3x-ui نسخه 3 به بالا) متصل می‌شود و ساخت، فروش و تمدید اکانت را به‌صورت خودکار انجام می‌دهد.

## قابلیت‌ها

- فروش و تمدید اکانت‌های MikroTik و ثنایی
- فروش OpenVPN، V2Ray یا هر دو سرویس به‌صورت هم‌زمان
- مدیریت بسته‌های مستقل و اکانت تست
- افزودن ریسلر و تعیین قیمت اختصاصی بر اساس هر گیگ فروش
- ثبت و مدیریت بدهی ریسلرها
- کیف پول کاربران
- سیستم Referral و بازاریابی
- پرداخت کارت‌به‌کارت و زرین‌پال
- مدیریت کامل تنظیمات از پنل ادمین تلگرام
- بکاپ خودکار و نگهداری اطلاعات در SQLite

## نصب روی Ubuntu 22.04 یا جدیدتر

<div dir="ltr" align="left">

```bash
curl -fsSL https://raw.githubusercontent.com/peyley95/account-sales-bot/main/install.sh | sudo bash
```

</div>

نصب‌کننده توکن ربات و Telegram ID عددی مدیر اصلی را دریافت می‌کند و سرویس را با systemd راه‌اندازی می‌کند. بعد از نصب، در تلگرام `/start` را بزنید و اتصال سرویس‌ها، درگاه‌های پرداخت و بسته‌ها را از پنل ادمین تنظیم کنید.

## مدیریت سرویس

<div dir="ltr" align="left">

```bash
sudo systemctl status account-sales-bot
sudo systemctl restart account-sales-bot
sudo journalctl -u account-sales-bot -f
```

</div>

به‌روزرسانی:

<div dir="ltr" align="left">

```bash
sudo bash /opt/account-sales-bot/update.sh
```

</div>

هیچ‌وقت توکن، رمزها، فایل ENV یا دیتابیس واقعی خود را در GitHub منتشر نکنید.

## درباره پروژه

صفر تا صد این ربات، از طراحی و کدنویسی تا تست و مستندسازی، با Codex در ChatGPT ساخته شده است.

این پروژه با مجوز [MIT](LICENSE) منتشر می‌شود.

</div>
