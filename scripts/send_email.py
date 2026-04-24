"""
send_email.py — Send the generated ACR Weekly Digest via SendGrid.

Reads the most recent build/email_*.html file and emails it to the recipient(s)
configured via environment variables.

Environment variables required:
    SENDGRID_API_KEY  — SendGrid API key (starts with SG.)
    EMAIL_FROM        — Verified sender address in SendGrid
    EMAIL_RECIPIENT   — Destination address (or comma-separated list)

Usage:
    python scripts/send_email.py
    python scripts/send_email.py --file build/email_2026-04-24.html  # explicit file
    python scripts/send_email.py --dry-run                           # validate without sending
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = REPO_ROOT / 'build'

SENDGRID_ENDPOINT = 'https://api.sendgrid.com/v3/mail/send'


def find_latest_email() -> Path:
    """Locate the most recent build/email_*.html file."""
    if not BUILD_DIR.exists():
        raise FileNotFoundError(
            f"Build directory {BUILD_DIR} doesn't exist. "
            f"Run build_email.py first."
        )
    files = sorted(BUILD_DIR.glob('email_*.html'), reverse=True)
    if not files:
        raise FileNotFoundError(
            f"No email_*.html files found in {BUILD_DIR}. "
            f"Run build_email.py first."
        )
    return files[0]


def parse_recipients(recipient_env: str) -> list[dict]:
    """Accept a single email or comma-separated list; return SendGrid's 'to' format."""
    emails = [e.strip() for e in recipient_env.split(',') if e.strip()]
    return [{'email': e} for e in emails]


def build_subject(html_path: Path) -> str:
    """Derive subject line from filename date.
    email_2026-04-24.html  ->  'ACR Weekly Digest — Apr 24, 2026'
    """
    stem = html_path.stem  # email_2026-04-24
    try:
        date_str = stem.split('_', 1)[1]  # 2026-04-24
        d = date.fromisoformat(date_str)
        return f'ACR Weekly Digest — {d.strftime("%b %d, %Y")}'
    except (IndexError, ValueError):
        return 'ACR Weekly Digest'


def send(api_key: str, from_email: str, recipients: list[dict],
         subject: str, html: str, dry_run: bool = False) -> None:
    """POST to SendGrid's /v3/mail/send endpoint."""
    payload = {
        'personalizations': [{'to': recipients}],
        'from': {'email': from_email, 'name': 'ACR Dashboard'},
        'subject': subject,
        'content': [{'type': 'text/html', 'value': html}],
    }

    if dry_run:
        print('DRY RUN — would send:')
        print(f'  From:    {from_email}')
        print(f'  To:      {", ".join(r["email"] for r in recipients)}')
        print(f'  Subject: {subject}')
        print(f'  Size:    {len(html) / 1024:.1f} KB')
        return

    response = requests.post(
        SENDGRID_ENDPOINT,
        headers={
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
        },
        json=payload,
        timeout=30,
    )

    if response.status_code >= 400:
        print(f'SendGrid returned HTTP {response.status_code}', file=sys.stderr)
        print(f'Response body: {response.text}', file=sys.stderr)
        response.raise_for_status()

    print(f'Sent: HTTP {response.status_code}')
    print(f'  From:    {from_email}')
    print(f'  To:      {", ".join(r["email"] for r in recipients)}')
    print(f'  Subject: {subject}')
    print(f'  Size:    {len(html) / 1024:.1f} KB')


def main():
    parser = argparse.ArgumentParser(description='Send the ACR weekly digest email.')
    parser.add_argument('--file', type=str,
                        help='Explicit path to HTML file (default: latest in build/)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Validate inputs and print what would be sent, but do not send')
    args = parser.parse_args()

    # Resolve the HTML file
    if args.file:
        html_path = Path(args.file)
        if not html_path.exists():
            sys.exit(f'File not found: {html_path}')
    else:
        html_path = find_latest_email()

    html = html_path.read_text()

    # Required env vars
    api_key = os.environ.get('SENDGRID_API_KEY')
    from_email = os.environ.get('EMAIL_FROM')
    recipient_env = os.environ.get('EMAIL_RECIPIENT')

    missing = [name for name, val in [
        ('SENDGRID_API_KEY', api_key),
        ('EMAIL_FROM', from_email),
        ('EMAIL_RECIPIENT', recipient_env),
    ] if not val]
    if missing:
        sys.exit(f'Missing environment variables: {", ".join(missing)}')

    recipients = parse_recipients(recipient_env)
    if not recipients:
        sys.exit('No valid recipients parsed from EMAIL_RECIPIENT')

    subject = build_subject(html_path)

    send(api_key, from_email, recipients, subject, html, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
