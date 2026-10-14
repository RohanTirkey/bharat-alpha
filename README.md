# BharatAlpha Live

Indian stock analyzer with live NSE candles (Yahoo Finance), Google News sentiment, and a broker-style web UI.

This is a **Python web app**, not a static HTML site. GitHub can store the code. **GitHub Pages cannot host it**, because the UI needs the Python API (`/api/analyze`, `/api/market-snapshot`).

## Local

```powershell
python bharat_alpha_live_server.py
```

Open http://localhost:8000/

## Put on GitHub

1. Create a new empty repository on GitHub (do not add a README).
2. In this folder:

```powershell
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git branch -M main
git push -u origin main
```

## Host the website (Render)

1. Sign up at [render.com](https://render.com) with GitHub.
2. **New → Web Service →** select this repo.
3. Settings:
   - **Runtime:** Python
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `python bharat_alpha_live_server.py`
4. Deploy. Render sets `PORT` automatically. Your public URL will look like `https://bharat-alpha.onrender.com`.

The free instance sleeps after idle time. The first request after sleep can take ~30–60 seconds.

Yahoo Finance sometimes blocks cloud IPs; if live candles fail, the app falls back to last known demo prices.
