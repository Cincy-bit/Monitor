# Monitor-App

A personal dashboard for tracking financial markets, news, your portfolio, weather, and Bitcoin miners on your local network — all in one customizable view. It also doubles as a lightweight Nostr client.

> **Platform:** macOS only (for now)

## Features

- 📈 Live prices for stocks, ETFs, indices, commodities, and crypto, with a customizable watchlist and scrolling ticker
- 📰 Aggregated news from multiple sources, sorted by category
- 💼 Personal portfolio tracking (cost basis, P/L) — stored locally, never sent anywhere
- ⛏️ Bitcoin miner monitoring for devices on your local network
- 🌦️ Weather by ZIP code or coordinates
- 🟣 Built-in Nostr client with adjustable panes

## Requirements

- macOS
- Python 3 (pre-installed on modern Macs — check with `python3 --version`)
- A modern web browser

No accounts, sign-ups, or paid API keys are required to run the dashboard. All optional third-party data sources used are free.

## Getting started

1. **Download this repo.**
   - Click **Code → Download ZIP** above and unzip it, or
   - Clone it with git:
     ```bash
     git clone https://github.com/Cincy-bit/Monitor-App.git
     ```
2. Keep `monitor.html` and `proxy.py` in the same folder — the server looks for `monitor.html` alongside itself.
3. Open **Terminal**, navigate to the folder, and run the server:
   ```bash
   cd path/to/Monitor-App
   python3 proxy.py
   ```
4. Open your browser to:
   ```
   http://127.0.0.1:8082
   ```
5. Keep the Terminal window open while using the dashboard. Press `Ctrl + C` in Terminal to stop the server.

### Optional: one-click launcher

To avoid retyping commands each time, save this as `start.command` in the same folder:

```bash
#!/bin/bash
cd "$(dirname "$0")"
python3 proxy.py
```

Then run `chmod +x start.command` once in Terminal. After that, double-clicking `start.command` starts the server.

## How it works

`proxy.py` runs a small local server on `127.0.0.1:8082` that serves the dashboard and proxies requests to public data sources (Yahoo Finance, Open-Meteo, RSS feeds, SEC EDGAR, and others) to work around CORS restrictions. It only binds to your local machine — it is not exposed to your network or the internet.

## Data & privacy

- Your watchlist, portfolio, ticker settings, and weather location are stored in your browser's local storage — on your machine only, never uploaded anywhere.
- If you use the miner-tracking feature, discovered miner IPs are saved to a local `miners.json` file created next to `proxy.py` the first time you add a miner. This file is machine-specific and isn't included in the repo — see `miners.example.json` for the expected format. Avoid committing your real `miners.json` if you fork or contribute to this repo.
- If you log into the Nostr client with a private key (nsec), it's stored in your browser's local storage in plain text. Use this only on a trusted device, or prefer a browser extension signer (NIP-07) when available.

## Optional: extended financial data

Financial statements pull from free public SEC EDGAR data by default — no setup needed. To use [Financial Modeling Prep](https://financialmodelingprep.com/) for extended data instead, set an API key as an environment variable before starting the server:

```bash
export FMP_API_KEY=your_key_here
python3 proxy.py
```

## Contributing

Issues and pull requests are welcome. Please avoid committing any personal data (API keys, `miners.json`, browser-exported settings) in contributions.

