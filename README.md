# ACR Registry Data Scraper

Automated daily scraper for the [American Carbon Registry (ACR)](https://acr2.apx.com/) public reports. Downloads Credit Status and Retired Credits data and publishes it as JSON for the ACR IFM Dashboard.

## How It Works

1. **GitHub Actions** runs a scheduled workflow daily at 6:00 AM ET
2. **Playwright** (headless Chrome) opens the ACR report pages and clicks the CSV download button
3. The CSVs are parsed and combined into a single `data/acr_data.json` file
4. If the data has changed, it's committed to this repo automatically
5. The dashboard fetches `data/acr_data.json` via GitHub Pages

## Data Sources

| Report | URL | Records |
|--------|-----|---------|
| Credit Status | [r=309](https://acr2.apx.com/myModule/rpt/myrpt.asp?r=309) | ~14,000+ |
| Retired Credits | [r=206](https://acr2.apx.com/myModule/rpt/myrpt.asp?r=206) | ~9,000+ |

## Setup

### 1. Create the GitHub Repo

1. Go to [github.com/new](https://github.com/new)
2. Name it `acr-dashboard` (or whatever you prefer)
3. Make it **Private** (recommended since this is for internal use)
4. Click "Create repository"

### 2. Push This Code

```bash
cd acr-scraper
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/acr-dashboard.git
git push -u origin main
```

### 3. Enable GitHub Pages (for hosting the dashboard)

1. Go to your repo → Settings → Pages
2. Under "Source", select **GitHub Actions**
3. The dashboard will be available at `https://YOUR_USERNAME.github.io/acr-dashboard/`

### 4. Enable the Workflow

1. Go to your repo → Actions tab
2. You should see the "Scrape ACR Registry Data" workflow
3. Click "Enable workflow" if prompted
4. Click "Run workflow" → "Run workflow" to test it manually

### 5. Verify

After the workflow runs, check that `data/acr_data.json` and `data/meta.json` were created. The meta file shows the last update timestamp and record counts.

## Running Locally

```bash
# Install dependencies
pip install playwright
playwright install chromium

# Run the scraper
python scripts/scrape.py

# Output will be in data/acr_data.json
```

## File Structure

```
├── .github/workflows/
│   └── scrape.yml          # GitHub Actions workflow (daily schedule)
├── scripts/
│   └── scrape.py           # Main scraper script
├── data/
│   ├── acr_data.json       # Full dataset (auto-generated)
│   └── meta.json           # Update metadata (auto-generated)
├── downloads/              # Temporary CSV downloads (gitignored)
└── README.md
```

## Troubleshooting

- **Workflow not running?** Check that Actions are enabled in repo Settings → Actions → General
- **Download timing out?** The ACR site may be slow. Increase `DOWNLOAD_TIMEOUT` in `scrape.py`
- **CSV format changed?** If ACR changes their column headers, update the field mappings in `process_credit_status()` and `process_retired_credits()`
- **Button not found?** If ACR redesigns the page, update the Playwright selector in `download_csv()`. Currently targets `img#downloadICon`
