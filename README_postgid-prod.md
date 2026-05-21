## PostGIS Production Connection Cheat Sheet

### Step 1: Fire Up the Secure Pipeline (In your local terminal)

Run this command to bridge your local machine to Cloudflare. Keep this terminal window open while you work.

```bash
cloudflared access tcp --hostname db-prod.yourdomain.com --listener localhost:5433

```

> 💡 **Pro-Tip:** If you want to run it silently in the background and keep your terminal free, run this instead:
> `nohup cloudflared access tcp --hostname db-prod.yourdomain.com --listener localhost:5433 > /dev/null 2>&1 &`

---

### Step 2: Open VS Code & Connect

Open your database extension in VS Code and launch your saved connection profile using these exact settings:

* **Host / Server:** `localhost` (or `127.0.0.1`)
* **Port:** `5433`
* **Database:** *[Your Prod DB Name]*
* **Username / Password:** *[Your Prod Postgres Credentials]*

---

### ⚠️ Troubleshooting: What if it fails to connect?

If you get a connection timeout or an authentication error, your security token has likely expired. Just re-authenticate in your browser by running this command, then try Step 1 again:

```bash
cloudflared access login https://db-prod.yourdomain.com

```