#!/usr/bin/env python3
"""TI parts crawler — MVP.

Pipeline:
  seed (part numbers) -> crawl (fetch product page, parse params, get datasheet URL)
  -> download-pdfs (fetch datasheets) -> export (CSV) / query.

Usage:
  python ti_crawler.py init
  python ti_crawler.py seed-file parts.txt
  python ti_crawler.py seed OPA1612 LM2596 TPS54302 MSP430G2553
  python ti_crawler.py crawl --limit 50
  python ti_crawler.py download-pdfs --limit 50
  python ti_crawler.py stats
  python ti_crawler.py export out.csv
"""
from __future__ import annotations

import json
import logging
import random
import re
import sqlite3
import sys
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import click
import httpx
from bs4 import BeautifulSoup
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


DB_PATH = Path("ti.db")
PDF_DIR = Path("datasheets")
LOG = logging.getLogger("ti_crawler")

PRODUCT_URL = "https://www.ti.com/product/{part}"
# TI's datasheet file names are lowercase. The /lit/gpn/<part> endpoint
# follows redirects to the actual PDF and is case-insensitive.
DATASHEET_URL = "https://www.ti.com/lit/ds/symlink/{part_lc}.pdf"
GENERIC_LIT_URL = "https://www.ti.com/lit/gpn/{part_lc}"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30.0
MIN_DELAY = 1.2
MAX_DELAY = 2.5


# ---------- database ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS parts (
    part_number TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    category TEXT,
    lifecycle TEXT,
    datasheet_url TEXT,
    pdf_local_path TEXT,
    pdf_status TEXT DEFAULT 'pending',   -- pending / done / error / skipped
    pdf_error TEXT,
    raw_json TEXT,
    crawl_status TEXT DEFAULT 'pending', -- pending / done / error
    crawl_error TEXT,
    fetched_at TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS parameters (
    part_number TEXT NOT NULL,
    param_name  TEXT NOT NULL,
    param_value TEXT,
    PRIMARY KEY (part_number, param_name)
);

CREATE INDEX IF NOT EXISTS idx_parts_crawl_status ON parts(crawl_status);
CREATE INDEX IF NOT EXISTS idx_parts_pdf_status   ON parts(pdf_status);
CREATE INDEX IF NOT EXISTS idx_parts_category     ON parts(category);
"""


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    with closing(db_conn()) as conn, conn:
        conn.executescript(SCHEMA)
    LOG.info("initialized %s", DB_PATH)


def seed_parts(parts: Iterable[str]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    added = 0
    with closing(db_conn()) as conn, conn:
        for raw in parts:
            p = raw.strip().upper()
            if not p:
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO parts(part_number, crawl_status, updated_at) "
                "VALUES (?, 'pending', ?)",
                (p, now),
            )
            added += cur.rowcount
    return added


# ---------- HTTP ----------


def make_client() -> httpx.Client:
    return httpx.Client(
        http2=True,
        timeout=REQUEST_TIMEOUT,
        headers={
            "User-Agent": USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        follow_redirects=True,
    )


def polite_sleep() -> None:
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


class TransientError(Exception):
    pass


@retry(
    retry=retry_if_exception_type(TransientError),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def _get(client: httpx.Client, url: str) -> httpx.Response:
    try:
        r = client.get(url)
    except httpx.RequestError as e:
        raise TransientError(str(e)) from e
    if r.status_code in (429, 500, 502, 503, 504):
        raise TransientError(f"http {r.status_code}")
    return r


# ---------- parsing ----------


def _text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


def parse_product_page(html: str) -> dict:
    """Extract structured data from a TI product page.

    Defensive: tries JSON-LD first, then page-level metadata, then a
    parameters table. Returns a dict with keys: name, description,
    category, lifecycle, datasheet_url, parameters (dict).
    """
    soup = BeautifulSoup(html, "lxml")
    out: dict = {
        "name": None,
        "description": None,
        "category": None,
        "lifecycle": None,
        "datasheet_url": None,
        "parameters": {},
    }

    # 1. JSON-LD (most reliable when present)
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "{}")
        except json.JSONDecodeError:
            continue
        nodes = data if isinstance(data, list) else [data]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("@type") in ("Product", "ProductModel"):
                out["name"] = out["name"] or node.get("name")
                out["description"] = out["description"] or node.get("description")
                cat = node.get("category")
                if isinstance(cat, list):
                    cat = " > ".join(str(c) for c in cat)
                out["category"] = out["category"] or cat

    # 2. og: meta fallbacks
    if not out["name"]:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            out["name"] = og["content"].strip()
    if not out["description"]:
        og = soup.find("meta", property="og:description")
        if og and og.get("content"):
            out["description"] = og["content"].strip()

    # 3. lifecycle: TI embeds it in a JS blob as marketingStatusDescription
    m = re.search(r'marketingStatusDescription["\s:]+["\']?([A-Z][A-Z_ ]+)', html)
    if m:
        out["lifecycle"] = m.group(1).strip()

    # 4. datasheet link — prefer /lit/gpn/<part> which TI redirects to the PDF
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/lit/gpn/" in href or ("/lit/ds/" in href and href.lower().endswith(".pdf")):
            out["datasheet_url"] = href if href.startswith("http") else f"https://www.ti.com{href}"
            break

    # 5. parameters from TI's custom web components
    #    <ti-multicolumn-list-row>
    #      <ti-multicolumn-list-cell><span>GBW (typ) (MHz)</span></ti-multicolumn-list-cell>
    #      <ti-multicolumn-list-cell><span>40</span></ti-multicolumn-list-cell>
    for row in soup.find_all("ti-multicolumn-list-row"):
        cells = row.find_all("ti-multicolumn-list-cell")
        if len(cells) >= 2:
            k = _text(cells[0])
            v = _text(cells[1])
            if k and v and len(k) < 120 and len(v) < 400:
                out["parameters"].setdefault(k, v)

    # 6. fallback: classic HTML tables (some TI pages still use them)
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                k = _text(cells[0])
                v = _text(cells[1])
                if k and v and len(k) < 120 and len(v) < 400:
                    out["parameters"].setdefault(k, v)

    return out


# ---------- crawling ----------


def crawl_one(client: httpx.Client, part: str) -> dict:
    url = PRODUCT_URL.format(part=part)
    r = _get(client, url)
    if r.status_code == 404:
        raise ValueError("product not found (404)")
    if r.status_code != 200:
        raise TransientError(f"http {r.status_code}")
    parsed = parse_product_page(r.text)
    if not parsed.get("datasheet_url"):
        # fallback to known URL pattern; verified on download
        parsed["datasheet_url"] = DATASHEET_URL.format(part_lc=part.lower())
    return parsed


def store_part(conn: sqlite3.Connection, part: str, parsed: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    params = parsed.pop("parameters", {})
    with conn:
        conn.execute(
            """
            UPDATE parts SET
                name = ?, description = ?, category = ?, lifecycle = ?,
                datasheet_url = ?, raw_json = ?, crawl_status = 'done',
                crawl_error = NULL, fetched_at = ?, updated_at = ?
            WHERE part_number = ?
            """,
            (
                parsed.get("name"),
                parsed.get("description"),
                parsed.get("category"),
                parsed.get("lifecycle"),
                parsed.get("datasheet_url"),
                json.dumps(parsed, ensure_ascii=False),
                now,
                now,
                part,
            ),
        )
        conn.execute("DELETE FROM parameters WHERE part_number = ?", (part,))
        if params:
            conn.executemany(
                "INSERT OR REPLACE INTO parameters(part_number, param_name, param_value) "
                "VALUES (?, ?, ?)",
                [(part, k, v) for k, v in params.items()],
            )


def mark_error(conn: sqlite3.Connection, part: str, msg: str) -> None:
    with conn:
        conn.execute(
            "UPDATE parts SET crawl_status='error', crawl_error=?, updated_at=datetime('now') "
            "WHERE part_number=?",
            (msg[:500], part),
        )


def crawl(limit: int, retry_errors: bool) -> None:
    where = "crawl_status='pending'"
    if retry_errors:
        where = "crawl_status IN ('pending','error')"
    with closing(db_conn()) as conn:
        rows = conn.execute(
            f"SELECT part_number FROM parts WHERE {where} ORDER BY part_number LIMIT ?",
            (limit,),
        ).fetchall()
    if not rows:
        click.echo("nothing to crawl")
        return
    click.echo(f"crawling {len(rows)} parts...")
    with make_client() as client, closing(db_conn()) as conn:
        for i, row in enumerate(rows, 1):
            part = row["part_number"]
            try:
                parsed = crawl_one(client, part)
                store_part(conn, part, parsed)
                click.echo(f"[{i}/{len(rows)}] OK  {part}  {parsed.get('name') or ''}")
            except ValueError as e:
                mark_error(conn, part, str(e))
                click.echo(f"[{i}/{len(rows)}] 404 {part}")
            except (TransientError, RetryError) as e:
                mark_error(conn, part, f"transient: {e}")
                click.echo(f"[{i}/{len(rows)}] ERR {part}  {e}")
            except Exception as e:  # noqa: BLE001
                mark_error(conn, part, f"parse: {e}")
                click.echo(f"[{i}/{len(rows)}] ERR {part}  {e}")
            polite_sleep()


# ---------- PDF download ----------


def download_pdfs(limit: int, retry_errors: bool) -> None:
    PDF_DIR.mkdir(exist_ok=True)
    where = "crawl_status='done' AND pdf_status='pending'"
    if retry_errors:
        where = "crawl_status='done' AND pdf_status IN ('pending','error')"
    with closing(db_conn()) as conn:
        rows = conn.execute(
            f"SELECT part_number, datasheet_url FROM parts WHERE {where} "
            "ORDER BY part_number LIMIT ?",
            (limit,),
        ).fetchall()
    if not rows:
        click.echo("no PDFs to download")
        return
    click.echo(f"downloading {len(rows)} datasheets...")
    with make_client() as client, closing(db_conn()) as conn:
        for i, row in enumerate(rows, 1):
            part = row["part_number"]
            url = row["datasheet_url"] or DATASHEET_URL.format(part_lc=part.lower())
            target = PDF_DIR / f"{part}.pdf"
            try:
                r = _get(client, url)
                ctype = r.headers.get("content-type", "")
                if r.status_code != 200 or "pdf" not in ctype.lower():
                    raise ValueError(f"not a pdf (status={r.status_code} type={ctype})")
                target.write_bytes(r.content)
                with conn:
                    conn.execute(
                        "UPDATE parts SET pdf_status='done', pdf_local_path=?, "
                        "pdf_error=NULL, updated_at=datetime('now') WHERE part_number=?",
                        (str(target), part),
                    )
                click.echo(f"[{i}/{len(rows)}] OK  {part}  ({len(r.content)//1024} KB)")
            except Exception as e:  # noqa: BLE001
                with conn:
                    conn.execute(
                        "UPDATE parts SET pdf_status='error', pdf_error=?, "
                        "updated_at=datetime('now') WHERE part_number=?",
                        (str(e)[:500], part),
                    )
                click.echo(f"[{i}/{len(rows)}] ERR {part}  {e}")
            polite_sleep()


# ---------- CLI ----------


@click.group()
@click.option("--db", default=str(DB_PATH), show_default=True, help="SQLite database path")
@click.option("-v", "--verbose", is_flag=True)
def cli(db: str, verbose: bool) -> None:
    global DB_PATH  # noqa: PLW0603
    DB_PATH = Path(db)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


@cli.command()
def init() -> None:
    """Create the SQLite database and tables."""
    init_db()
    click.echo(f"initialized {DB_PATH}")


@cli.command()
@click.argument("parts", nargs=-1, required=True)
def seed(parts: tuple[str, ...]) -> None:
    """Add part numbers to the crawl queue (space-separated)."""
    init_db()
    n = seed_parts(parts)
    click.echo(f"added {n} new parts")


@cli.command("seed-file")
@click.argument("path", type=click.Path(exists=True, dir_okay=False))
def seed_file(path: str) -> None:
    """Add part numbers from a text file (one per line, # for comments)."""
    init_db()
    with open(path, encoding="utf-8") as f:
        parts = [line.split("#", 1)[0].strip() for line in f]
    n = seed_parts(p for p in parts if p)
    click.echo(f"added {n} new parts from {path}")


@cli.command()
@click.option("--limit", default=50, show_default=True)
@click.option("--retry-errors", is_flag=True, help="also retry parts in 'error' state")
def crawl_cmd(limit: int, retry_errors: bool) -> None:
    """Fetch product pages for pending parts."""
    crawl(limit, retry_errors)


cli.add_command(crawl_cmd, name="crawl")


@cli.command("download-pdfs")
@click.option("--limit", default=50, show_default=True)
@click.option("--retry-errors", is_flag=True)
def download_pdfs_cmd(limit: int, retry_errors: bool) -> None:
    """Download datasheet PDFs for crawled parts."""
    download_pdfs(limit, retry_errors)


@cli.command()
def stats() -> None:
    """Show progress counters."""
    with closing(db_conn()) as conn:
        rows = conn.execute(
            "SELECT crawl_status, COUNT(*) c FROM parts GROUP BY crawl_status"
        ).fetchall()
        pdf = conn.execute(
            "SELECT pdf_status, COUNT(*) c FROM parts WHERE crawl_status='done' "
            "GROUP BY pdf_status"
        ).fetchall()
        params = conn.execute("SELECT COUNT(*) c FROM parameters").fetchone()
    click.echo("crawl:")
    for r in rows:
        click.echo(f"  {r['crawl_status']:10s} {r['c']}")
    click.echo("pdfs (of crawled):")
    for r in pdf:
        click.echo(f"  {r['pdf_status']:10s} {r['c']}")
    click.echo(f"parameters rows: {params['c']}")


@cli.command()
@click.argument("path", type=click.Path(dir_okay=False))
def export(path: str) -> None:
    """Export crawled parts to CSV (one row per part, parameters as JSON)."""
    import csv

    with closing(db_conn()) as conn:
        rows = conn.execute(
            "SELECT part_number, name, description, category, lifecycle, "
            "datasheet_url, pdf_local_path, pdf_status, crawl_status "
            "FROM parts WHERE crawl_status='done'"
        ).fetchall()
        params_by_part: dict[str, dict[str, str]] = {}
        for r in conn.execute("SELECT part_number, param_name, param_value FROM parameters"):
            params_by_part.setdefault(r["part_number"], {})[r["param_name"]] = r["param_value"]

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "part_number", "name", "description", "category", "lifecycle",
            "datasheet_url", "pdf_local_path", "pdf_status", "parameters_json",
        ])
        for r in rows:
            w.writerow([
                r["part_number"], r["name"], r["description"], r["category"],
                r["lifecycle"], r["datasheet_url"], r["pdf_local_path"],
                r["pdf_status"],
                json.dumps(params_by_part.get(r["part_number"], {}), ensure_ascii=False),
            ])
    click.echo(f"exported {len(rows)} parts to {path}")


if __name__ == "__main__":
    cli()
