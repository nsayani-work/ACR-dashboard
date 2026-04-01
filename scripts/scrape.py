"""
ACR Registry Data Scraper
Downloads Credit Status and Retired Credits CSVs from the ACR public registry,
processes them into JSON, and saves to the data/ directory.

Uses Playwright to handle the browser-based CSV export.
"""

import asyncio
import csv
import json
import os
import glob
import time
from datetime import datetime, timezone
from pathlib import Path

# Config
CREDIT_STATUS_URL = "https://acr2.apx.com/myModule/rpt/myrpt.asp?r=309"
RETIRED_CREDITS_URL = "https://acr2.apx.com/myModule/rpt/myrpt.asp?r=206"
DOWNLOAD_DIR = Path(__file__).parent.parent / "downloads"
OUTPUT_DIR = Path(__file__).parent.parent / "data"
DOWNLOAD_TIMEOUT = 120_000  # 2 minutes per download


async def download_csv(page, url, label):
    """Navigate to a report page and click the CSV download button."""
    print(f"[{label}] Navigating to {url}")
    await page.goto(url, wait_until="networkidle", timeout=60_000)
    
    # Wait for the download icon to be visible
    print(f"[{label}] Waiting for download button...")
    download_icon = page.locator('img#downloadICon')
    await download_icon.wait_for(state="visible", timeout=30_000)
    
    # Click and wait for download
    print(f"[{label}] Clicking download...")
    async with page.expect_download(timeout=DOWNLOAD_TIMEOUT) as download_info:
        await download_icon.click()
    
    download = await download_info.value
    
    # Save to downloads directory
    dest = DOWNLOAD_DIR / f"{label}.csv"
    await download.save_as(str(dest))
    print(f"[{label}] Downloaded to {dest} ({dest.stat().st_size / 1024:.0f} KB)")
    return dest


def parse_csv(filepath, label):
    """Parse a downloaded CSV file into a list of dicts."""
    records = []
    with open(filepath, "r", encoding="utf-8-sig") as f:
        # ACR CSVs sometimes have a header row with the report date, then the actual headers
        lines = f.readlines()
    
    # Find the header row (the one with 'Credit Serial Numbers' or similar)
    header_idx = 0
    for i, line in enumerate(lines):
        if "Credit Serial" in line or "Serial Number" in line or "Project ID" in line:
            header_idx = i
            break
        # Also check for common column names
        if "Quantity" in line and "Project" in line:
            header_idx = i
            break
    
    # If we couldn't find a clear header, try the first or second row
    if header_idx == 0 and len(lines) > 1:
        # Check if first row looks like a date/title row
        if lines[0].count(",") < 3:
            header_idx = 1
    
    print(f"[{label}] Using header at row {header_idx}")
    
    reader = csv.DictReader(lines[header_idx:])
    headers = reader.fieldnames
    print(f"[{label}] Columns: {headers}")
    
    for row in reader:
        records.append(dict(row))
    
    print(f"[{label}] Parsed {len(records)} records")
    return records, headers


def normalize_date(s):
    """Convert any date string to YYYY-MM-DD format."""
    if not s:
        return ""
    s = s.strip()
    # Try parsing various formats
    from datetime import datetime as dt
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y"]:
        try:
            d = dt.strptime(s[:min(len(s), 22)].strip(), fmt)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: just return first 10 chars
    return s[:10]


def process_credit_status(records):
    """Process Credit Status records into compact JSON format."""
    processed = []
    for r in records:
        try:
            qty_key = next((k for k in r.keys() if "Quantity" in k and "Credit" in k), None)
            qty = int(float(r.get(qty_key, 0) or 0)) if qty_key else 0
            
            serial_key = next((k for k in r.keys() if "Serial" in k), None)
            serial = (r.get(serial_key, "") or "").strip() if serial_key else ""
            
            processed.append({
                "sn": serial,
                "q": qty,
                "d": normalize_date(r.get("Date Issued (GMT)", r.get("Date Issued", ""))),
                "v": int(float(r.get("Vintage", 0) or 0)),
                "dev": (r.get("Project\xa0Developer", r.get("Project Developer", "")) or "")[:60],
                "s": (r.get("Status", "") or "").strip(),
                "pid": (r.get("Project ID", "") or "").strip(),
                "pn": (r.get("Project Name", r.get(" Project Name ", "")) or "").strip()[:80],
                "pt": (r.get("Project Type", "") or "").strip(),
                "m": (r.get("Project Methodology/Protocol", "") or "").strip()[:80],
                "mv": (r.get("Methodology/Protocol Version", "") or "").strip(),
                "st": (r.get("Project Site State", "") or "").strip(),
                "co": (r.get("Project Site Country", "") or "").strip(),
                "vr": (r.get("Verified Removal", "") or "").strip(),
                "ccp": (r.get("CCP Approved", "") or "").strip(),
            })
        except Exception as e:
            print(f"  Warning: skipping row due to error: {e}")
            continue
    
    return processed


def process_retired_credits(records):
    """Process Retired Credits records into compact JSON format."""
    processed = []
    for r in records:
        try:
            qty_key = next((k for k in r.keys() if "Quantity" in k), None)
            qty = int(float(r.get(qty_key, 0) or 0)) if qty_key else 0
            
            serial_key = next((k for k in r.keys() if "Serial" in k), None)
            serial = (r.get(serial_key, "") or "").strip() if serial_key else ""
            
            processed.append({
                "sn": serial,
                "q": qty,
                "d": normalize_date(r.get("Status Effective (GMT)", r.get("Status Effective", ""))),
                "di": normalize_date(r.get("Date Issued (GMT)", r.get("Date Issued", ""))),
                "v": int(float(r.get("Vintage", 0) or 0)),
                "b": (r.get("Retired on Behalf of", r.get("Retired on Behalf Of", "")) or "").strip()[:80],
                "p": (r.get("Purpose of Retirement", "") or "").strip()[:200],
                "pid": (r.get("Project ID", "") or "").strip(),
                "pn": (r.get("Project Name", "") or "").strip()[:80],
                "pt": (r.get("Project Type", "") or "").strip(),
                "m": (r.get("Project Methodology/Protocol", "") or "").strip()[:80],
                "mv": (r.get("Methodology/Protocol Version", "") or "").strip(),
                "st": (r.get("Project Site State", "") or "").strip(),
                "vr": (r.get("Verified Removal", "") or "").strip(),
                "ccp": (r.get("CCP Approved", "") or "").strip(),
            })
        except Exception as e:
            print(f"  Warning: skipping row due to error: {e}")
            continue
    
    return processed


def add_oha_columns(credits, retired):
    """
    Replicate the OHA calculated columns from the Excel workbook:
    
    1. Status Effective Date (OHA): For retired credits in the Credit Status tab,
       look up the retirement date from the Retired Credits tab by matching on
       Credit Serial Numbers. Non-retired credits get "NA".
    
    2. Days From Issuance to Retirement (OHA): Status Effective Date minus
       Date Issued. Gives the number of days between issuance and retirement.
    """
    from datetime import datetime as dt
    
    # Build a lookup: serial number -> retirement effective date
    retirement_dates = {}
    for r in retired:
        if r["sn"] and r["d"]:
            retirement_dates[r["sn"]] = r["d"]
    
    print(f"[OHA columns] Built retirement date lookup with {len(retirement_dates)} entries")
    
    matched = 0
    for c in credits:
        if c["s"] == "Retired" and c["sn"] in retirement_dates:
            ret_date_str = retirement_dates[c["sn"]]
            c["sed"] = ret_date_str  # Status Effective Date
            
            # Calculate days from issuance to retirement
            try:
                issued = dt.strptime(c["d"][:10], "%Y-%m-%d")
                retired_dt = dt.strptime(ret_date_str[:10], "%Y-%m-%d")
                days = (retired_dt - issued).days
                c["dtr"] = days  # Days to Retirement
            except (ValueError, TypeError):
                c["dtr"] = None
            
            matched += 1
        else:
            c["sed"] = None
            c["dtr"] = None
    
    print(f"[OHA columns] Matched {matched} retired credits with retirement dates")
    return credits


async def main():
    from playwright.async_api import async_playwright
    
    # Setup directories
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        
        # Download both reports
        cs_path = await download_csv(page, CREDIT_STATUS_URL, "credit_status")
        rc_path = await download_csv(page, RETIRED_CREDITS_URL, "retired_credits")
        
        await browser.close()
    
    # Parse CSVs
    cs_records, cs_headers = parse_csv(cs_path, "credit_status")
    rc_records, rc_headers = parse_csv(rc_path, "retired_credits")
    
    # Process into compact format
    credits = process_credit_status(cs_records)
    retired = process_retired_credits(rc_records)
    
    # Add OHA calculated columns (cross-tab join)
    credits = add_oha_columns(credits, retired)
    
    # Build output
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output = {
        "updated_at": now,
        "credit_status_count": len(credits),
        "retired_credits_count": len(retired),
        "credits": credits,
        "retired": retired,
    }
    
    # Write JSON
    out_path = OUTPUT_DIR / "acr_data.json"
    with open(out_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\nOutput: {out_path} ({size_mb:.1f} MB)")
    print(f"  Credit Status: {len(credits)} records")
    print(f"  Retired Credits: {len(retired)} records")
    print(f"  Updated: {now}")
    
    # Also write a small metadata file
    meta = {
        "updated_at": now,
        "credit_status_count": len(credits),
        "retired_credits_count": len(retired),
    }
    with open(OUTPUT_DIR / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
