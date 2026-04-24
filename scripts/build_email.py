"""
build_email.py — ACR Weekly Digest generator

Reads two ACR snapshots (this Friday and last Friday), diffs them to find new
registry activity, classifies IFM vs Non-IFM by OHA's category rules, generates
four PNG charts, and renders the email HTML from a Jinja2 template.

Usage:
    python scripts/build_email.py                        # auto-detect most recent two Fridays
    python scripts/build_email.py --this 2026-04-24 --last 2026-04-17   # explicit dates

Output:
    build/email_YYYY-MM-DD.html   — self-contained HTML with base64-embedded charts
    build/charts/*.png            — individual chart PNGs (also embedded in HTML)
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.lines as mlines
import matplotlib.dates as mdates
from PIL import Image
from jinja2 import Environment, FileSystemLoader, select_autoescape

# ============================================================================
# Configuration
# ============================================================================
REPO_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOTS_DIR = REPO_ROOT / 'data' / 'snapshots'
TEMPLATES_DIR = REPO_ROOT / 'templates'
BUILD_DIR = REPO_ROOT / 'build'
CHARTS_DIR = BUILD_DIR / 'charts'

DASHBOARD_URL = 'https://nsayani-work.github.io/ACR-dashboard/ACR_IFM_Dashboard_v6.html'

# Thresholds (credits per event to appear in the "Events >= X" detail tables)
IFM_THRESHOLD = 10_000
NONIFM_THRESHOLD = 25_000

# Status filter for issuance (retirements are all by definition)
ISSUANCE_STATUSES = {'Active', 'Retired'}

# Chart styling
GREEN = '#10b981'
RED = '#dc2626'
SLATE = '#334155'
MUTE = '#64748b'
GRID = '#e2e8f0'
LIGHT = '#f1f5f9'
BG = '#fafbfc'
CHART_DPI = 144


# ============================================================================
# Classification logic (mirrors the dashboard)
# ============================================================================

def is_ifm(record: dict) -> bool:
    m = record.get('m', '') or ''
    return 'Improved Forest Management' in m or m.startswith('IFM')


def ifm_category(record: dict) -> str | None:
    """Classify an IFM record into one of 4 buckets.
    Rules:
      IFM + Version 2.1 + VR=Yes -> Dynamic Removals
      IFM + Version 2.1 + VR!=Yes -> Dynamic Conservation
      IFM + Version!=2.1 + VR=Yes -> Static Removals
      IFM + Version!=2.1 + VR!=Yes -> Static Conservation
    """
    if not is_ifm(record):
        return None
    mv = record.get('mv', '') or ''
    vr = record.get('vr', '') or ''
    is_v21 = 'Version 2.1' in mv
    is_vr = (vr == 'Yes')
    if is_v21 and is_vr:
        return 'Dynamic Removals'
    if is_v21 and not is_vr:
        return 'Dynamic Conservation'
    if not is_v21 and is_vr:
        return 'Static Removals'
    return 'Static Conservation'


def is_ifm_static(r):
    return ifm_category(r) in ('Static Conservation', 'Static Removals')


def is_ifm_dynamic(r):
    return ifm_category(r) in ('Dynamic Conservation', 'Dynamic Removals')


# Methodology labels for Non-IFM records. Keyed off project type (pt) with a
# couple of overrides based on methodology name (m).
def nonifm_methodology(record: dict) -> str | None:
    if is_ifm(record):
        return None
    pt = record.get('pt', '') or ''
    m = record.get('m', '') or ''
    pt_map = {
        'Ozone Depleting Substances': 'ODS Destruction',
        'Landfill Gas Capture & Combustion': 'Landfill Gas',
        'Industrial Process Emissions': 'Industrial Process Emissions',
        'Carbon Capture & Storage (CCS)': 'Carbon Capture & Storage',
        'Transport / Fleet Efficiency': 'Transport / Fleet Efficiency',
        'Coal Mine Methane': 'Coal Mine Methane',
        'Agricultural Land Management': 'Agricultural Land Mgmt',
        'Livestock Waste Management': 'Livestock Waste Mgmt',
        'Industrial Gas Substitution': 'Industrial Gas Substitution',
        'Renewable Energy': 'Renewable Energy',
        'Fuel Switching': 'Fuel Switching',
        'Wetland Restoration': 'Wetland Restoration',
        'Energy Efficiency': 'Energy Efficiency',
        'Wastewater Treatment': 'Wastewater Treatment',
    }
    if pt in pt_map:
        return pt_map[pt]
    if pt == 'Forest Carbon':
        if 'Afforestation' in m or 'Reforestation' in m:
            return 'Afforestation / Reforestation'
        if 'ARB Compliance' in m:
            return 'ARB US Forest (non-IFM)'
        return 'Forest Carbon (other)'
    return f'Other ({pt})' if pt else 'Other (uncategorized)'


# ============================================================================
# Snapshot loading and diffing
# ============================================================================

def load_snapshot(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def find_recent_fridays(n: int = 2) -> list[Path]:
    """Return the N most recent Friday snapshot paths, newest first.
    Falls back to nearest-before-Friday if a Friday file is missing.
    """
    if not SNAPSHOTS_DIR.exists():
        raise FileNotFoundError(
            f"{SNAPSHOTS_DIR} doesn't exist. Has the snapshot archiver run yet? "
            f"Check the scrape workflow."
        )
    files = sorted(SNAPSHOTS_DIR.glob('*.json'), reverse=True)
    if not files:
        raise FileNotFoundError(f"No snapshots found in {SNAPSHOTS_DIR}")

    # Find Fridays in filenames (YYYY-MM-DD.json, weekday=4)
    fridays = []
    for f in files:
        try:
            d = date.fromisoformat(f.stem)
            if d.weekday() == 4:  # Friday
                fridays.append(f)
        except ValueError:
            continue
        if len(fridays) == n:
            break

    if len(fridays) < n:
        # Fall back: just use the N newest files, even if not Fridays
        print(f"Warning: only found {len(fridays)} Friday snapshots. "
              f"Falling back to the {n} most recent files available.", file=sys.stderr)
        return files[:n]
    return fridays


def diff_snapshots(this_wk: dict, last_wk: dict) -> tuple[list, list]:
    """Return (new_credits, new_retired) — records that appear in this_wk but not last_wk."""
    last_credits_sn = {c['sn'] for c in last_wk['credits']}
    last_retired_sn = {r['sn'] for r in last_wk['retired']}

    new_credits = [c for c in this_wk['credits']
                   if c['sn'] not in last_credits_sn
                   and c.get('s') in ISSUANCE_STATUSES]
    new_retired = [r for r in this_wk['retired'] if r['sn'] not in last_retired_sn]

    return new_credits, new_retired


# ============================================================================
# Aggregations
# ============================================================================

def tally_by_ifm_category(records: list[dict]) -> dict[str, int]:
    out = defaultdict(int)
    for r in records:
        cat = ifm_category(r)
        if cat:
            out[cat] += r.get('q', 0)
    for cat in ['Static Conservation', 'Static Removals',
                'Dynamic Conservation', 'Dynamic Removals']:
        out.setdefault(cat, 0)
    return dict(out)


def tally_by_methodology(records: list[dict]) -> dict[str, int]:
    out = defaultdict(int)
    for r in records:
        meth = nonifm_methodology(r)
        if meth:
            out[meth] += r.get('q', 0)
    return dict(out)


def ytd_tally(records: list[dict], year: int, classifier) -> dict[str, int]:
    """Tally records whose date field starts with the given year, by classifier."""
    prefix = f'{year}-'
    out = defaultdict(int)
    for r in records:
        d = r.get('d', '') or ''
        if d.startswith(prefix):
            cat = classifier(r)
            if cat:
                out[cat] += r.get('q', 0)
    return dict(out)


def daily_cumulative(records: list[dict], year: int, filter_func,
                     end_date: date) -> list[tuple[date, int]]:
    """Build a daily cumulative series from Jan 1 of `year` through `end_date`."""
    daily = defaultdict(int)
    for r in records:
        d = r.get('d', '') or ''
        if d.startswith(f'{year}-') and filter_func(r):
            try:
                date_obj = date.fromisoformat(d)
                daily[date_obj] += r.get('q', 0)
            except ValueError:
                continue

    if not daily:
        return [(date(year, 1, 1), 0), (end_date, 0)]

    start = date(year, 1, 1)
    result = []
    cum = 0
    current = start
    while current <= end_date:
        cum += daily.get(current, 0)
        result.append((current, cum))
        current += timedelta(days=1)
    return result


# ============================================================================
# Chart generation
# ============================================================================

def _style_axes(ax):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(GRID)
    ax.spines['bottom'].set_color(GRID)
    ax.tick_params(colors=MUTE, labelsize=7, length=0)
    ax.grid(True, axis='y', color=LIGHT, linestyle='--', linewidth=0.6)
    ax.set_axisbelow(True)
    ax.set_facecolor(BG)


def _fmt_short(x, _):
    if x >= 1_000_000:
        return f'{x/1_000_000:.1f}M'
    if x >= 1_000:
        return f'{x/1_000:.0f}k'
    return f'{int(x)}'


def _fmt_end(n):
    if n >= 1_000_000:
        return f'{n/1_000_000:.2f}M'
    if n >= 1_000:
        return f'{n/1000:.0f}k'
    return f'{n:,}'


def line_chart(path: Path, issued: list[tuple[date, int]],
               retired: list[tuple[date, int]], y_max: int,
               snapshot_end: date):
    fig, ax = plt.subplots(figsize=(3.3, 2.0), dpi=CHART_DPI)
    fig.patch.set_facecolor('white')

    if issued:
        xi, yi = zip(*issued)
        ax.plot(xi, yi, color=GREEN, linewidth=2.0)
        issued_end = yi[-1]
    else:
        issued_end = 0
    if retired:
        xr, yr = zip(*retired)
        ax.plot(xr, yr, color=RED, linewidth=2.0)
        retired_end = yr[-1]
    else:
        retired_end = 0

    _style_axes(ax)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    year = snapshot_end.year
    ax.set_xlim(date(year, 1, 1) - timedelta(days=3), snapshot_end + timedelta(days=3))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_fmt_short))
    ax.set_ylim(0, y_max)

    handles = [
        mlines.Line2D([], [], color=GREEN, linewidth=2.0,
                      label=f'Issued ({_fmt_end(issued_end)})'),
        mlines.Line2D([], [], color=RED, linewidth=2.0,
                      label=f'Retired ({_fmt_end(retired_end)})'),
    ]
    ax.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, -0.12),
              ncol=2, fontsize=7, frameon=False, labelcolor=SLATE,
              handlelength=1.5, handletextpad=0.4, columnspacing=1.5)

    plt.tight_layout(pad=0.5)
    plt.savefig(path, dpi=CHART_DPI, bbox_inches='tight', facecolor='white')
    plt.close()

    _compress_png(path)


def bar_chart(path: Path, data: list[tuple[str, int]], color: str):
    """data = [(label, value), ...] sorted with largest first (will be top bar)."""
    labels = [d[0] for d in data]
    values = [d[1] for d in data]

    fig, ax = plt.subplots(figsize=(3.3, 2.0), dpi=CHART_DPI)
    fig.patch.set_facecolor('white')

    y_pos = list(range(len(labels)))[::-1]
    ax.barh(y_pos, values, color=color, height=0.65, edgecolor='none')

    max_val = max(values) if values else 1
    for y, v in zip(y_pos, values):
        ax.text(v + max_val * 0.02, y, f'{v:,}', va='center', ha='left',
                fontsize=7, color=SLATE)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=7.5, color=SLATE)
    ax.tick_params(axis='y', length=0, pad=2)
    ax.tick_params(axis='x', colors=MUTE, labelsize=7, length=0)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color(GRID)
    ax.grid(True, axis='x', color=LIGHT, linestyle='--', linewidth=0.6)
    ax.set_axisbelow(True)
    ax.set_facecolor(BG)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_short))
    ax.set_xlim(0, max_val * 1.3)

    plt.tight_layout(pad=0.5)
    plt.savefig(path, dpi=CHART_DPI, bbox_inches='tight', facecolor='white')
    plt.close()

    _compress_png(path)


def _compress_png(path: Path):
    """Palette-compress to shrink file size ~5x (matters for email size limits)."""
    img = Image.open(path)
    img.convert('P', palette=Image.ADAPTIVE, colors=64).save(path, optimize=True)


def _png_to_data_uri(path: Path) -> str:
    data = base64.b64encode(path.read_bytes()).decode()
    return f'data:image/png;base64,{data}'


# ============================================================================
# Event-detail tables
# ============================================================================

def ifm_events_above_threshold(records: list[dict], threshold: int) -> list[dict]:
    """Return records classified as IFM with q >= threshold, sorted by q desc."""
    events = [r for r in records if is_ifm(r) and r.get('q', 0) >= threshold]
    return sorted(events, key=lambda x: -x.get('q', 0))


def nonifm_events_above_threshold(records: list[dict], threshold: int) -> list[dict]:
    events = [r for r in records if not is_ifm(r) and r.get('q', 0) >= threshold]
    return sorted(events, key=lambda x: -x.get('q', 0))


def top_n_by(records: list[dict], key_field: str, filter_func, n: int = 5) -> list[tuple[str, int]]:
    """Aggregate q by key_field (e.g., 'dev' for developer, 'b' for retirement account)."""
    totals = defaultdict(int)
    for r in records:
        if filter_func(r):
            key = r.get(key_field, '') or 'Unknown'
            totals[key] += r.get('q', 0)
    return sorted(totals.items(), key=lambda x: -x[1])[:n]


# ============================================================================
# Template rendering
# ============================================================================

def render_email(context: dict, output_path: Path):
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=select_autoescape(['html'])
    )
    env.filters['thousands'] = lambda n: f'{n:,}' if n else '—'
    template = env.get_template('email_template.html.j2')
    html = template.render(**context)
    output_path.write_text(html)
    print(f'Wrote {output_path} ({output_path.stat().st_size / 1024:.1f} KB)')


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Build ACR weekly digest email.')
    parser.add_argument('--this', type=str, help='YYYY-MM-DD for this Friday (optional)')
    parser.add_argument('--last', type=str, help='YYYY-MM-DD for last Friday (optional)')
    args = parser.parse_args()

    # Resolve snapshot files
    if args.this and args.last:
        this_path = SNAPSHOTS_DIR / f'{args.this}.json'
        last_path = SNAPSHOTS_DIR / f'{args.last}.json'
        if not this_path.exists() or not last_path.exists():
            raise FileNotFoundError(f'Missing snapshots: {this_path} or {last_path}')
    else:
        recent = find_recent_fridays(2)
        this_path, last_path = recent[0], recent[1]
        print(f'Auto-detected: this={this_path.name}, last={last_path.name}')

    this_date = date.fromisoformat(this_path.stem)
    last_date = date.fromisoformat(last_path.stem)

    this_wk = load_snapshot(this_path)
    last_wk = load_snapshot(last_path)

    # Diff for this week's new activity
    new_credits, new_retired = diff_snapshots(this_wk, last_wk)
    print(f'Diff: {len(new_credits)} new issuances, {len(new_retired)} new retirements')

    # ------------------------------------------------------------------------
    # IFM summary table data
    # ------------------------------------------------------------------------
    valid_credits_this = [c for c in this_wk['credits']
                          if c.get('s') in ISSUANCE_STATUSES]

    ifm_context = {
        'issued_this_wk': tally_by_ifm_category(new_credits),
        'retired_this_wk': tally_by_ifm_category(new_retired),
        'issued_ytd': ytd_tally(valid_credits_this, this_date.year, ifm_category),
        'retired_ytd': ytd_tally(this_wk['retired'], this_date.year, ifm_category),
        'issued_last_year': ytd_tally(valid_credits_this, this_date.year - 1, ifm_category),
        'retired_last_year': ytd_tally(this_wk['retired'], this_date.year - 1, ifm_category),
    }
    for key in list(ifm_context.keys()):
        # Ensure all 4 categories are present so template doesn't KeyError
        for cat in ['Static Conservation', 'Static Removals',
                    'Dynamic Conservation', 'Dynamic Removals']:
            ifm_context[key].setdefault(cat, 0)

    # IFM totals for KPI strip
    ifm_issued_total_wk = sum(ifm_context['issued_this_wk'].values())
    ifm_retired_total_wk = sum(ifm_context['retired_this_wk'].values())
    ifm_issue_event_count = sum(1 for c in new_credits if is_ifm(c))
    ifm_retire_event_count = sum(1 for r in new_retired if is_ifm(r))

    # ------------------------------------------------------------------------
    # Non-IFM summary table data
    # ------------------------------------------------------------------------
    nonifm_issued_wk = tally_by_methodology(new_credits)
    nonifm_retired_wk = tally_by_methodology(new_retired)
    nonifm_issued_ytd = ytd_tally(valid_credits_this, this_date.year, nonifm_methodology)
    nonifm_retired_ytd = ytd_tally(this_wk['retired'], this_date.year, nonifm_methodology)
    nonifm_issued_ly = ytd_tally(valid_credits_this, this_date.year - 1, nonifm_methodology)
    nonifm_retired_ly = ytd_tally(this_wk['retired'], this_date.year - 1, nonifm_methodology)

    def merge_methodologies(wk_d, ytd_d, ly_d):
        """Build ordered list of (methodology, wk_val, ytd_val, ly_val) sorted by YTD desc."""
        all_methods = set(wk_d) | set(ytd_d) | set(ly_d)
        ordered = sorted(all_methods, key=lambda m: -ytd_d.get(m, 0))
        return [(m, wk_d.get(m, 0), ytd_d.get(m, 0), ly_d.get(m, 0)) for m in ordered]

    nonifm_issued_rows = merge_methodologies(nonifm_issued_wk, nonifm_issued_ytd, nonifm_issued_ly)
    nonifm_retired_rows = merge_methodologies(nonifm_retired_wk, nonifm_retired_ytd, nonifm_retired_ly)

    nonifm_issued_total_wk = sum(nonifm_issued_wk.values())
    nonifm_retired_total_wk = sum(nonifm_retired_wk.values())
    nonifm_issue_event_count = sum(1 for c in new_credits if not is_ifm(c))
    nonifm_retire_event_count = sum(1 for r in new_retired if not is_ifm(r))

    # ------------------------------------------------------------------------
    # Charts
    # ------------------------------------------------------------------------
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    # Compute daily cumulative series and auto-scale y-axis
    static_issued = daily_cumulative(valid_credits_this, this_date.year, is_ifm_static, this_date)
    static_retired = daily_cumulative(this_wk['retired'], this_date.year, is_ifm_static, this_date)
    dynamic_issued = daily_cumulative(valid_credits_this, this_date.year, is_ifm_dynamic, this_date)
    dynamic_retired = daily_cumulative(this_wk['retired'], this_date.year, is_ifm_dynamic, this_date)

    def auto_ymax(*series_list):
        max_val = 0
        for s in series_list:
            if s:
                max_val = max(max_val, max(v for _, v in s))
        # Round up to next clean increment
        if max_val < 100_000:
            return max(max_val * 1.15, 10_000)
        if max_val < 1_000_000:
            return round(max_val * 1.15 / 100_000) * 100_000
        return round(max_val * 1.15 / 500_000) * 500_000

    line_chart(CHARTS_DIR / 'ifm_static.png', static_issued, static_retired,
               y_max=auto_ymax(static_issued, static_retired), snapshot_end=this_date)
    line_chart(CHARTS_DIR / 'ifm_dynamic.png', dynamic_issued, dynamic_retired,
               y_max=auto_ymax(dynamic_issued, dynamic_retired), snapshot_end=this_date)

    nonifm_issue_bars = sorted(nonifm_issued_wk.items(), key=lambda x: -x[1])[:6]
    nonifm_retire_bars = sorted(nonifm_retired_wk.items(), key=lambda x: -x[1])[:6]
    bar_chart(CHARTS_DIR / 'nonifm_issuance.png', nonifm_issue_bars, GREEN)
    bar_chart(CHARTS_DIR / 'nonifm_retirements.png', nonifm_retire_bars, RED)

    # ------------------------------------------------------------------------
    # Events above threshold
    # ------------------------------------------------------------------------
    ifm_issue_events = ifm_events_above_threshold(new_credits, IFM_THRESHOLD)
    ifm_retire_events = ifm_events_above_threshold(new_retired, IFM_THRESHOLD)
    nonifm_issue_events = nonifm_events_above_threshold(new_credits, NONIFM_THRESHOLD)
    nonifm_retire_events = nonifm_events_above_threshold(new_retired, NONIFM_THRESHOLD)

    # Add methodology label for non-IFM events (template needs it)
    for e in nonifm_issue_events + nonifm_retire_events:
        e['_methodology'] = nonifm_methodology(e)

    # ------------------------------------------------------------------------
    # Top issuers/retirees
    # ------------------------------------------------------------------------
    top_ifm_issuers = top_n_by(new_credits, 'dev', is_ifm, n=5)
    top_nonifm_issuers = top_n_by(new_credits, 'dev', lambda r: not is_ifm(r), n=5)
    top_ifm_retirees = top_n_by(new_retired, 'b', is_ifm, n=5)
    top_nonifm_retirees = top_n_by(new_retired, 'b', lambda r: not is_ifm(r), n=5)

    # ------------------------------------------------------------------------
    # Narrative (template-based, no LLM)
    # ------------------------------------------------------------------------
    narrative_parts = []
    narrative_parts.append(
        f'{len(ifm_issue_events)} ACR IFM issuance event'
        f'{"s" if len(ifm_issue_events) != 1 else ""} and '
        f'{len(ifm_retire_events)} retirement event'
        f'{"s" if len(ifm_retire_events) != 1 else ""} cleared the '
        f'{IFM_THRESHOLD:,} threshold this week.'
    )
    narrative_parts.append(
        f'On the Non-IFM side, {len(nonifm_issue_events)} issuance event'
        f'{"s" if len(nonifm_issue_events) != 1 else ""} and '
        f'{len(nonifm_retire_events)} retirement event'
        f'{"s" if len(nonifm_retire_events) != 1 else ""} cleared '
        f'the {NONIFM_THRESHOLD:,} threshold.'
    )
    if nonifm_issue_events:
        top = nonifm_issue_events[0]
        narrative_parts.append(
            f"The week's largest new issuance was a {top['q']:,}-credit "
            f"{nonifm_methodology(top)} project ({top.get('pid', 'n/a')})."
        )
    if nonifm_retire_events:
        top = nonifm_retire_events[0]
        narrative_parts.append(
            f"The largest Non-IFM retirement was {top['q']:,} credits retired "
            f"by {top.get('b', 'unknown')}."
        )
    narrative = ' '.join(narrative_parts)

    # ------------------------------------------------------------------------
    # Build final context dict for template
    # ------------------------------------------------------------------------
    context = {
        'week_start': (this_date - timedelta(days=7)).strftime('%b %d'),
        'week_end': this_date.strftime('%b %d, %Y'),
        'year_current': this_date.year,
        'year_prior': this_date.year - 1,
        'dashboard_url': DASHBOARD_URL,
        'narrative': narrative,

        # KPI strip
        'ifm_issued_total_wk': ifm_issued_total_wk,
        'ifm_retired_total_wk': ifm_retired_total_wk,
        'nonifm_issued_total_wk': nonifm_issued_total_wk,
        'nonifm_retired_total_wk': nonifm_retired_total_wk,
        'ifm_issue_event_count': ifm_issue_event_count,
        'ifm_retire_event_count': ifm_retire_event_count,
        'nonifm_issue_event_count': nonifm_issue_event_count,
        'nonifm_retire_event_count': nonifm_retire_event_count,

        # IFM table (flatten to list of 4 categories with columns)
        'ifm_rows_issued': [
            (cat,
             ifm_context['issued_this_wk'][cat],
             ifm_context['issued_ytd'][cat],
             ifm_context['issued_last_year'][cat])
            for cat in ['Static Conservation', 'Static Removals',
                        'Dynamic Conservation', 'Dynamic Removals']
        ],
        'ifm_rows_retired': [
            (cat,
             ifm_context['retired_this_wk'][cat],
             ifm_context['retired_ytd'][cat],
             ifm_context['retired_last_year'][cat])
            for cat in ['Static Conservation', 'Static Removals',
                        'Dynamic Conservation', 'Dynamic Removals']
        ],
        'ifm_issued_totals': (ifm_issued_total_wk,
                              sum(ifm_context['issued_ytd'].values()),
                              sum(ifm_context['issued_last_year'].values())),
        'ifm_retired_totals': (ifm_retired_total_wk,
                               sum(ifm_context['retired_ytd'].values()),
                               sum(ifm_context['retired_last_year'].values())),

        # Non-IFM table
        'nonifm_issued_rows': nonifm_issued_rows,
        'nonifm_retired_rows': nonifm_retired_rows,
        'nonifm_issued_totals': (nonifm_issued_total_wk,
                                 sum(nonifm_issued_ytd.values()),
                                 sum(nonifm_issued_ly.values())),
        'nonifm_retired_totals': (nonifm_retired_total_wk,
                                  sum(nonifm_retired_ytd.values()),
                                  sum(nonifm_retired_ly.values())),

        # Top issuers/retirees
        'top_ifm_issuers': top_ifm_issuers,
        'top_nonifm_issuers': top_nonifm_issuers,
        'top_ifm_retirees': top_ifm_retirees,
        'top_nonifm_retirees': top_nonifm_retirees,

        # Event detail tables
        'ifm_issue_events': ifm_issue_events,
        'ifm_retire_events': ifm_retire_events,
        'nonifm_issue_events': nonifm_issue_events,
        'nonifm_retire_events': nonifm_retire_events,
        'ifm_threshold': IFM_THRESHOLD,
        'nonifm_threshold': NONIFM_THRESHOLD,

        # Embedded chart data URIs
        'chart_ifm_static': _png_to_data_uri(CHARTS_DIR / 'ifm_static.png'),
        'chart_ifm_dynamic': _png_to_data_uri(CHARTS_DIR / 'ifm_dynamic.png'),
        'chart_nonifm_issuance': _png_to_data_uri(CHARTS_DIR / 'nonifm_issuance.png'),
        'chart_nonifm_retirements': _png_to_data_uri(CHARTS_DIR / 'nonifm_retirements.png'),
    }

    # Render
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    output_path = BUILD_DIR / f'email_{this_date.isoformat()}.html'
    render_email(context, output_path)


if __name__ == '__main__':
    main()
