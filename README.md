# Monitor

A personal dashboard for tracking markets, news, your portfolio, weather, and Bitcoin miners — all in one customizable view. It also doubles as a lightweight Nostr client. Enable Nostr Wallet Connect to zap posts, paste and pay invoices, or search and pay any lightning address.

> **Platform:** macOS only (for now)

## Features

- Live prices for stocks, ETFs, indices, commodities, and crypto, with a customizable watchlist and scrolling ticker
- Aggregated news from multiple sources, sorted by category
- Personal portfolio tracking (cost basis, P/L) — stored locally, never sent anywhere
- Bitcoin miner monitoring for devices on your local network
- Weather by ZIP code or coordinates, including a live NOAA radar with basemap
- Built-in Nostr client with adjustable panes
- Nostr Wallet Connect to zap posts, pay invoices or any lightning address

## Requirements

- macOS
- Python 3 (pre-installed on modern Macs — check with `python3 --version`)
- Pillow (`pip3 install Pillow`) — used to build the weather radar's basemap image
- A modern web browser

No accounts, sign-ups, or paid API keys are required to run the dashboard. All optional third-party data sources used are free.

## Getting started

1. **Download this repo.**
  - Click **Code → Download ZIP** above and unzip it, or
  - Clone it with git:

```
git clone https://github.com/Cincy-bit/Monitor.git
```

2. Keep `monitor.html` and `proxy.py` in the same folder — the server looks for `monitor.html` alongside itself.
3. Install Pillow, needed for the weather radar's basemap image:

```
pip3 install Pillow
```

4. Open **Terminal**, navigate to the folder, and run the server:

```
cd path/to/Monitor
python3 proxy.py
```

5. Open your browser to:

```
http://127.0.0.1:8082
```

6. Keep the Terminal window open while using the dashboard. Press `Ctrl + C` in Terminal to stop the server.

### Optional: one-click launcher

To avoid retyping commands each time, save this as `start.command` in the same folder:

```
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
- The Nostr client supports three login methods: a browser extension signer (NIP-07) or remote signer (NIP-46), where your private key never touches this page at all, or pasting a private key (nsec) directly. If you use nsec, the key is encrypted at rest with a device-bound key (not plain text) by default, with an optional passphrase lock for stronger protection in Settings → Nostr.
- When Nostr‑Wallet‑Connect is enabled, the dashboard sends only signed NWC JSON payloads to the relay you configure. Those payloads are part of the Nostr protocol and contain no personally identifying information beyond the public key you provide. The relay cannot read or alter the payload without breaking the cryptographic signature, and you are free to run your own relay to eliminate any third‑party involvement.

## Optional: weather radar basemap

The Live Radar view (Weather → Live Radar) works with no setup, but tiles will carry an "API key required" watermark instead of state/coastline lines until you set a free CARTO API key. Get one (no account needed, ~1 minute) at [carto.com/basemaps/apikey](https://carto.com/basemaps/apikey), then either set it as an environment variable:

```
export CARTO_API_KEY=your_key_here
python3 proxy.py
```

or save it to a file named `carto_api_key.txt` in the same folder as `proxy.py`, containing just the key and nothing else — useful since a shell profile edit only takes effect in new terminal sessions. The environment variable wins if both are set. Avoid committing your real `carto_api_key.txt` if you fork or contribute to this repo.

## Optional: extended financial data

Financial statements pull from free public SEC EDGAR data by default — no setup needed. To use [Financial Modeling Prep](https://financialmodelingprep.com/) for extended data instead, set an API key as an environment variable before starting the server:

```
export FMP_API_KEY=your_key_here
python3 proxy.py
```

## Contributing

Issues and pull requests are welcome. Please avoid committing any personal data (API keys, `carto_api_key.txt`, `miners.json`, browser-exported settings) in contributions.
