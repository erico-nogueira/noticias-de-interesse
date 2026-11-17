from __future__ import annotations

import html
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import feedparser
import psycopg
import requests

USER_AGENT = "Mozilla/5.0 (compatible; GameNewsBot/1.0; +https://github.com)"
MAX_ENTRIES_PER_SOURCE = 30
MAX_AGE_DAYS = 14        # ignora itens mais antigos que isso
RETENTION_DAYS = 90      # apaga artigos mais antigos que isso (poupa espaço grátis)
REQUEST_TIMEOUT = 20

TAG_RE = re.compile(r"<[^>]+>")
IMG_RE = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)
SPACE_RE = re.compile(r"\s+")


def clean_text(raw: str | None, limit: int = 400) -> str | None:
    if not raw:
        return None
    text = SPACE_RE.sub(" ", html.unescape(TAG_RE.sub(" ", raw))).strip()
    if not text:
        return None
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def entry_image(entry) -> str | None:
    for key in ("media_thumbnail", "media_content"):
        for item in entry.get(key, []) or []:
            if item.get("url"):
                return item["url"]
    for enc in entry.get("enclosures", []) or []:
        if str(enc.get("type", "")).startswith("image") and enc.get("href"):
            return enc["href"]
    body = entry.get("summary", "") or ""
    for content in entry.get("content", []) or []:
        body += content.get("value", "")
    match = IMG_RE.search(body)
    return match.group(1) if match else None


def entry_date(entry) -> datetime:
    for key in ("published_parsed", "updated_parsed"):
        value = entry.get(key)
        if value:
            return datetime.fromtimestamp(time.mktime(value), tz=timezone.utc)
    return datetime.now(timezone.utc)


def fetch_feed(url: str):
    resp = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/atom+xml, */*"},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def main() -> int:
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("ERRO: defina a variável SUPABASE_DB_URL (secret do GitHub).", file=sys.stderr)
        return 1

    started = datetime.now(timezone.utc)
    cutoff = started - timedelta(days=MAX_AGE_DAYS)
    ok = failed = inserted = 0
    errors: list[dict] = []

    # prepare_threshold=None é necessário com o pooler (pgbouncer) na porta 6543
    with psycopg.connect(db_url, prepare_threshold=None, autocommit=True) as conn:
        sources = conn.execute(
            "select id, name, feed_url, language, section, topic from public.sources where active"
        ).fetchall()
        print(f"{len(sources)} fontes ativas.")

        for source_id, name, feed_url, language, section, topic in sources:
            try:
                feed = fetch_feed(feed_url)
                if feed.bozo and not feed.entries:
                    raise ValueError(f"feed inválido: {feed.bozo_exception}")

                new_here = 0
                for entry in feed.entries[:MAX_ENTRIES_PER_SOURCE]:
                    link = (entry.get("link") or "").strip()
                    title = clean_text(entry.get("title"), 300)
                    published = entry_date(entry)
                    if not link or not title or published < cutoff:
                        continue
                    publisher = (entry.get("source") or {}).get("title") or name
                    cur = conn.execute(
                        """
                        insert into public.articles
                          (source_id, title, summary, url, image_url, publisher,
                           language, section, topic, published_at)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on conflict (url) do nothing
                        """,
                        (source_id, title, clean_text(entry.get("summary")), link,
                         entry_image(entry), publisher, language, section, topic, published),
                    )
                    new_here += cur.rowcount
                inserted += new_here
                ok += 1
                print(f"[ok]   {name}: +{new_here}")
            except Exception as exc:  # noqa: BLE001 - uma fonte ruim não pode parar as outras
                failed += 1
                errors.append({"source": name, "error": str(exc)[:300]})
                print(f"[erro] {name}: {exc}", file=sys.stderr)

        conn.execute(
            "delete from public.articles where published_at < now() - make_interval(days => %s)",
            (RETENTION_DAYS,),
        )
        conn.execute(
            """
            insert into public.pipeline_runs
              (started_at, finished_at, sources_ok, sources_failed, articles_inserted, errors)
            values (%s, now(), %s, %s, %s, %s)
            """,
            (started, ok, failed, inserted, json.dumps(errors)),
        )

    print(f"Fim: {ok} fontes ok, {failed} com erro, {inserted} artigos novos.")
    return 1 if ok == 0 and failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
