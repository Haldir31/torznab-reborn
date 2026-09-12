"""
reborn-torznab — Torznab indexer backed by the stream-fusion-reborn database.

Exposes a Torznab API (/api?t=caps|search|movie|tvsearch) that Prowlarr / Sonarr /
Radarr can register as a "Generic Torznab" indexer. Search hits the Postgres
table `torrent_items` (populated by stream-fusion-reborn's tracker sync) and
returns magnet releases. The *arr apps then grab the magnet and hand it to their
download client (decypharr).
"""
import os
import time
import re
import html
import datetime
import urllib.parse
from email.utils import formatdate
from typing import Optional

import asyncpg
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, PlainTextResponse

PG_DSN = os.environ.get("PG_DSN", "")
API_KEY = os.environ.get("TORZNAB_API_KEY", "").strip()
DEFAULT_LIMIT = int(os.environ.get("TORZNAB_LIMIT", "200"))
HARD_LIMIT = int(os.environ.get("TORZNAB_HARD_LIMIT", "500"))
MIN_SEEDERS = int(os.environ.get("TORZNAB_MIN_SEEDERS", "0"))
LANGUAGES = [x.strip().lower() for x in os.environ.get("TORZNAB_LANGUAGES", "").split(",") if x.strip()]
CACHED_ONLY = os.environ.get("TORZNAB_CACHED_ONLY", "").lower() in ("1", "true", "yes", "on")
CACHED_SERVICE = os.environ.get("TORZNAB_CACHED_SERVICE", "alldebrid").strip().lower()
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()
TITLE = os.environ.get("TORZNAB_TITLE", "Reborn (stream-fusion)")

# --- Cache warming: replay Torznab searches as StreamFusion /stream requests ---
WARM_CACHE = os.environ.get("WARM_CACHE", "true").lower() in ("1", "true", "yes", "on")
STREMIO_HOST = os.environ.get("STREMIO_HOST", "http://streamfusion:8080").rstrip("/")
STREMIO_CONFIG = os.environ.get("STREMIO_CONFIG", "").strip()
if not STREMIO_CONFIG:
    try:
        with open(os.environ.get("STREMIO_CONFIG_FILE", "/data/stremio_config")) as _f:
            STREMIO_CONFIG = _f.read().strip()
    except Exception:
        STREMIO_CONFIG = ""
WARM_TTL = int(os.environ.get("WARM_TTL", str(12 * 3600)))          # don't re-warm the same id within N seconds
WARM_CONCURRENCY = int(os.environ.get("WARM_CONCURRENCY", "3"))
WARM_TIMEOUT = int(os.environ.get("WARM_TIMEOUT", "120"))

# --- Synchronous cached-only search -----------------------------------------
# When on, a movie/tv search asks StreamFusion for the streams of that exact
# title (imdb[:season:ep]), WAITS for its AllDebrid cache check to finish, and
# returns ONLY the releases confirmed cached (⚡instant). Sonarr/Radarr then
# grab one of those -> decypharr -> AllDebrid = always an instant symlink, and
# decypharr never submits a non-cached magnet (no zombie-magnet buildup).
STREMIO_SYNC = os.environ.get("STREMIO_SYNC_SEARCH", "").lower() in ("1", "true", "yes", "on")
STREMIO_SYNC_TIMEOUT = int(os.environ.get("STREMIO_SYNC_TIMEOUT", "90"))
STREMIO_SYNC_CONCURRENCY = int(os.environ.get("STREMIO_SYNC_CONCURRENCY", "4"))
STREMIO_SYNC_TTL = int(os.environ.get("STREMIO_SYNC_TTL", "600"))     # reuse a title's stream list within N seconds
STREMIO_SYNC_FALLBACK = os.environ.get("STREMIO_SYNC_FALLBACK", "db").strip().lower()  # db | empty
# When StreamFusion reports 0 cached releases for a title, still return up to this
# many top (non-cached) releases so the *arr can grab one and let AllDebrid
# download it (needs decypharr `download_uncached: true`). 0 = strict cached-only.
STREMIO_SYNC_UNCACHED = int(os.environ.get("STREMIO_SYNC_UNCACHED", "0"))

# --- Import-list (Radarr/Sonarr "Custom List") defaults ---
LIST_DAYS = int(os.environ.get("LIST_DAYS", "30"))            # only titles with a release added in the last N days (0 = all-time)
LIST_MIN_SEEDERS = int(os.environ.get("LIST_MIN_SEEDERS", "5"))
LIST_LANGUAGES = [x.strip().lower() for x in os.environ.get("LIST_LANGUAGES", "fr,multi").split(",") if x.strip()]
LIST_RESOLUTIONS = [x.strip().lower() for x in os.environ.get("LIST_RESOLUTIONS", "").split(",") if x.strip()]
LIST_LIMIT = int(os.environ.get("LIST_LIMIT", "2000"))
LIST_HARD_LIMIT = int(os.environ.get("LIST_HARD_LIMIT", "20000"))

import asyncio
import json as _json

CACHE_FILE = os.environ.get("CACHE_FILE", "/data/cache.json")

app = FastAPI(title="reborn-torznab")
pool: Optional[asyncpg.Pool] = None
_tvdb_cache: dict[str, Optional[str]] = {}
_orig_lang_cache: dict = {}
_tmdb_tvdb_cache: dict = {}
_warm_seen: dict = {}                 # stream_id -> epoch of last warm
_warm_sem: Optional[asyncio.Semaphore] = None
_sync_sem: Optional[asyncio.Semaphore] = None
_sync_cache: dict = {}               # "type/id" -> (epoch, streams list)


def _load_cache():
    try:
        with open(CACHE_FILE) as f:
            d = _json.load(f)
        _tvdb_cache.update(d.get("tvdb", {}))
        _orig_lang_cache.update(d.get("orig_lang", {}))
        _tmdb_tvdb_cache.update({int(k): v for k, v in d.get("tmdb_tvdb", {}).items()})
        _warm_seen.update(d.get("warm_seen", {}))
    except Exception:
        pass


def _save_cache():
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        tmp = CACHE_FILE + ".tmp"
        # prune warm-seen entries older than 2*TTL
        cut = time.time() - 2 * WARM_TTL
        for k in [k for k, v in _warm_seen.items() if v < cut]:
            _warm_seen.pop(k, None)
        with open(tmp, "w") as f:
            _json.dump(
                {"tvdb": _tvdb_cache, "orig_lang": _orig_lang_cache,
                 "tmdb_tvdb": _tmdb_tvdb_cache, "warm_seen": _warm_seen},
                f,
            )
        os.replace(tmp, CACHE_FILE)
    except Exception:
        pass


async def _warm_stream(stream_type: str, stream_id: str):
    """
    Fire a StreamFusion /stream request so it checks the AllDebrid cache for this
    title's torrents and persists the result in stream-fusion's debrid_cache.
    Fire-and-forget; deduped per stream_id within WARM_TTL.
    """
    if not (WARM_CACHE and STREMIO_CONFIG):
        return
    now = time.time()
    if now - _warm_seen.get(stream_id, 0) < WARM_TTL:
        return
    _warm_seen[stream_id] = now
    url = f"{STREMIO_HOST}/{STREMIO_CONFIG}/stream/{stream_type}/{stream_id}.json"
    try:
        async with _warm_sem:
            async with httpx.AsyncClient(timeout=WARM_TIMEOUT) as c:
                await c.get(url)
    except Exception:
        _warm_seen.pop(stream_id, None)   # allow a retry next time


# --------------------------------------------------------------------------- #
# synchronous cached-only search (StreamFusion /stream -> instant releases only)
# --------------------------------------------------------------------------- #
_SF_SIZE_RE = re.compile(r"([\d]+(?:[.,]\d+)?)\s*([KMGT]i?B)\b", re.I)
_SF_SEED_RE = re.compile(r"👥\s*(\d+)")
_SF_UNIT = {
    "kb": 1_000, "mb": 1_000_000, "gb": 1_000_000_000, "tb": 1_000_000_000_000,
    "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4,
}


def _sf_is_instant(stream: dict) -> bool:
    n = (stream.get("name") or "").lower()
    return "⚡" in (stream.get("name") or "") or "instant" in n


def _sf_size(desc: str) -> int:
    # description holds e.g. "💾 67.23GB" ; take the first size-looking token
    for m in _SF_SIZE_RE.finditer(desc or ""):
        try:
            return int(float(m.group(1).replace(",", ".")) * _SF_UNIT.get(m.group(2).lower(), 1_000_000_000))
        except Exception:
            continue
    return 0


def _sf_seeders(desc: str) -> int:
    m = _SF_SEED_RE.search(desc or "")
    return int(m.group(1)) if m else 0


def _sf_tags(name: str, desc: str) -> list[str]:
    """stream-fusion-style language tags parsed from the stream name/description
    so _languages() scores them like a real torrent_items row."""
    blob = f"{name}\n{desc}".lower()
    tags: list[str] = []
    if "multi" in blob:
        tags.append("multi")
    if re.search(r"🇫🇷|truefrench|\bfrench\b|\bvff\b|\bvfq\b|\bvf2\b|\bvfi\b|\bvfb\b|\bvfr\b|\bvf\b", blob):
        tags.append("fr")
    if "vostfr" in blob:
        tags.append("vostfr")
    if not tags:
        if re.search(r"\bvostfr\b|\bvo\b|\bsubfrench\b", blob):
            tags.append("original")
        else:
            tags.append("multi")   # SF already filtered to the config's fr/multi langs
    return tags


async def _stremio_rows(media_type: str, imdb: str, tmdb: Optional[str],
                        season: Optional[int], ep: Optional[int], limit: int):
    """
    Ask StreamFusion for this title's streams, wait for its AllDebrid cache
    check, and return row-dicts for the releases confirmed cached (⚡instant).
    Returns None on transport failure (caller falls back).
    """
    if not (STREMIO_CONFIG and _sync_sem):
        return None
    if media_type == "series":
        sid = f"{imdb}:{season if season is not None else 1}:{ep if ep is not None else 1}"
        stype = "series"
    else:
        sid = imdb
        stype = "movie"

    ck = f"{stype}/{sid}"
    now = time.time()
    cached = _sync_cache.get(ck)
    if cached and now - cached[0] < STREMIO_SYNC_TTL:
        streams = cached[1]
    else:
        url = f"{STREMIO_HOST}/{STREMIO_CONFIG}/stream/{stype}/{sid}.json"
        try:
            async with _sync_sem:
                async with httpx.AsyncClient(timeout=STREMIO_SYNC_TIMEOUT) as c:
                    r = await c.get(url)
            streams = ((r.json() or {}).get("streams")) or []
            _sync_cache[ck] = (now, streams)
            if len(_sync_cache) > 4000:
                for k, _ in sorted(_sync_cache.items(), key=lambda kv: kv[1][0])[:1000]:
                    _sync_cache.pop(k, None)
        except Exception:
            return None

    tmdb_id = int(tmdb) if (tmdb or "").isdigit() else None

    def _row(st: dict, cached: bool) -> Optional[dict]:
        ih = (st.get("infoHash") or "").strip().lower()
        if not ih:
            return None
        desc = st.get("description") or ""
        bh = st.get("behaviorHints") or {}
        title = (bh.get("filename") or "").strip()
        if not title and desc:
            title = desc.splitlines()[0].strip()
        title = re.sub(r"\.(mkv|mp4|avi|ts|m2ts|mov|wmv)$", "", title or ih, flags=re.I)
        return {
            "id": None,
            "raw_title": title,
            "size": _sf_size(desc),
            "info_hash": ih,
            "magnet": None,
            "link": None,
            "seeders": _sf_seeders(desc) or 1,
            "languages": _sf_tags(st.get("name") or "", desc),
            "indexer": "reborn-cache" if cached else "reborn-uncached",
            "type": media_type,
            "imdb_id": imdb,
            "tmdb_id": tmdb_id,
            "created_at": now,
            "parsed_data": {},
        }

    rows: list = []
    seen: set = set()
    for st in streams:
        if not _sf_is_instant(st):
            continue
        r = _row(st, cached=True)
        if not r or r["info_hash"] in seen:
            continue
        seen.add(r["info_hash"])
        rows.append(r)
        if len(rows) >= limit:
            break

    # No cached release for this title: optionally return the top N non-cached
    # ones so the *arr can still grab (AllDebrid then downloads it; needs
    # decypharr `download_uncached: true`).
    if not rows and STREMIO_SYNC_UNCACHED > 0:
        for st in streams:
            if _sf_is_instant(st):
                continue
            r = _row(st, cached=False)
            if not r or r["info_hash"] in seen:
                continue
            seen.add(r["info_hash"])
            rows.append(r)
            if len(rows) >= STREMIO_SYNC_UNCACHED:
                break

    await _attach_original_languages(rows)
    return rows


@app.on_event("startup")
async def _startup():
    global pool, _warm_sem, _sync_sem
    if not PG_DSN:
        raise RuntimeError(
            "PG_DSN is not set. Configure it (see .env.example), e.g. "
            "postgresql://<user>:<password>@<host>:5432/<database>"
        )
    pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=1, max_size=10, command_timeout=30)
    _warm_sem = asyncio.Semaphore(WARM_CONCURRENCY)
    _sync_sem = asyncio.Semaphore(STREMIO_SYNC_CONCURRENCY)
    _load_cache()

    async def _saver():
        while True:
            await asyncio.sleep(300)
            _save_cache()

    asyncio.create_task(_saver())

    async def _prewarm_sonarr():
        """Resolve every series' tmdb->tvdb in the background so /list/sonarr stays fast."""
        await asyncio.sleep(5)
        try:
            rows = await _list_titles(
                "series", days=LIST_DAYS, min_seeders=LIST_MIN_SEEDERS,
                languages=LIST_LANGUAGES, resolutions=LIST_RESOLUTIONS, limit=LIST_HARD_LIMIT,
            )
            todo = [r["tmdb_id"] for r in rows if r["tmdb_id"] not in _tmdb_tvdb_cache]
            if not todo or not TMDB_API_KEY:
                return
            sem = asyncio.Semaphore(25)
            async with httpx.AsyncClient(timeout=8) as c:
                async def one(tid):
                    async with sem:
                        await _tmdb_to_tvdb(c, tid)
                for i in range(0, len(todo), 500):
                    await asyncio.gather(*(one(t) for t in todo[i:i + 500]))
                    _save_cache()
        except Exception:
            pass

    asyncio.create_task(_prewarm_sonarr())


@app.on_event("shutdown")
async def _shutdown():
    _save_cache()
    if pool:
        await pool.close()


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _norm_imdb(v: str) -> Optional[str]:
    if not v:
        return None
    v = v.strip().lower()
    m = re.search(r"(\d{6,9})", v)
    if not m:
        return None
    return "tt" + m.group(1).zfill(7)


def _magnet(row) -> Optional[str]:
    ih = (row["info_hash"] or "").strip()
    for cand in (row.get("magnet"), row.get("link")):
        if cand and cand.startswith("magnet:"):
            return cand
    if not ih:
        return None
    return f"magnet:?xt=urn:btih:{ih}&dn={urllib.parse.quote(row['raw_title'] or ih)}"


def _category(t: str) -> str:
    return "5000" if t == "series" else "2000"


# ISO 639-1 -> Sonarr/Radarr language name
_ISO_NAME = {
    "fr": "French", "en": "English", "es": "Spanish", "de": "German", "it": "Italian",
    "pt": "Portuguese", "ru": "Russian", "ja": "Japanese", "ko": "Korean", "zh": "Chinese",
    "nl": "Dutch", "pl": "Polish", "sv": "Swedish", "da": "Danish", "no": "Norwegian",
    "nb": "Norwegian", "fi": "Finnish", "cs": "Czech", "el": "Greek", "hi": "Hindi",
    "ar": "Arabic", "tr": "Turkish", "hu": "Hungarian", "ro": "Romanian", "he": "Hebrew",
    "th": "Thai", "uk": "Ukrainian", "vi": "Vietnamese", "id": "Indonesian", "ta": "Tamil",
    "te": "Telugu", "ml": "Malayalam", "bn": "Bengali", "fa": "Persian", "bg": "Bulgarian",
    "hr": "Croatian", "sr": "Serbian", "sk": "Slovak", "sl": "Slovenian", "et": "Estonian",
    "lv": "Latvian", "lt": "Lithuanian", "ca": "Catalan", "is": "Icelandic", "mk": "Macedonian",
}
# explicit French-audio tags from stream-fusion
_FR_TAGS = {"fr", "vff", "vfq", "vf", "vf2", "vfi", "vfb", "truefrench", "vfr"}
# "audio is the media's original language" tags
_ORIG_TAGS = {"original", "vo", "vostfr", "vostfr", "subfrench", "vost"}


def _languages(row, orig_lang: Optional[str] = None) -> list[str]:
    """
    Map stream-fusion `languages` tags to concrete language names.
    `orig_lang` is the media's original audio language (ISO 639-1) resolved from
    TMDB — used for `original`/`vo`/`vostfr` tags where the audio language is
    whatever the title was originally made in (NOT necessarily English).
    """
    raw = row.get("languages") or []
    if isinstance(raw, str):
        raw = [x.strip() for x in raw.strip("{}").split(",") if x.strip()]
    tags = {str(x).strip().lower() for x in raw}
    orig_name = _ISO_NAME.get((orig_lang or "").lower())

    out: list[str] = []

    def add(name):
        if name and name not in out:
            out.append(name)

    if tags & _FR_TAGS:
        add("French")
    if "multi" in tags:
        add("French")          # a MULTi release always carries a French track on this stack
        add(orig_name)         # ...plus the original-language track (if we know it)
    if tags & _ORIG_TAGS:
        add(orig_name)         # audio = original language; unknown -> nothing (let *arr parse the title)
    for t in tags:
        if t in _ISO_NAME:
            add(_ISO_NAME[t])

    # last-resort title heuristic when the DB gave us nothing usable
    if not out:
        t = (row.get("raw_title") or "").lower()
        if re.search(r"\b(multi|truefrench|french|vff|vfq|vf2|vfi)\b", t):
            add("French")
            if "multi" in t:
                add(orig_name)
    return out


async def _original_language(session, imdb_id, tmdb_id, media_type) -> Optional[str]:
    """Media's original audio language (ISO 639-1) via TMDB, cached."""
    if not TMDB_API_KEY:
        return None
    key = f"t{tmdb_id}" if tmdb_id else (f"i{imdb_id}" if imdb_id else None)
    if not key:
        return None
    if key in _orig_lang_cache:
        return _orig_lang_cache[key]
    lang = None
    kind = "tv" if media_type in ("series", "show", "tv") else "movie"
    try:
        if tmdb_id:
            r = await session.get(
                f"https://api.themoviedb.org/3/{kind}/{tmdb_id}",
                params={"api_key": TMDB_API_KEY},
            )
            if r.status_code == 200:
                lang = (r.json() or {}).get("original_language")
        if not lang and imdb_id:
            r = await session.get(
                f"https://api.themoviedb.org/3/find/{imdb_id}",
                params={"api_key": TMDB_API_KEY, "external_source": "imdb_id"},
            )
            d = r.json() or {}
            res = (d.get("movie_results") or []) + (d.get("tv_results") or [])
            if res:
                lang = res[0].get("original_language")
    except Exception:
        lang = None
    _orig_lang_cache[key] = lang
    return lang


async def _attach_original_languages(rows):
    if not TMDB_API_KEY or not rows:
        return
    async with httpx.AsyncClient(timeout=8) as c:
        seen: dict = {}
        for r in rows:
            k = r.get("tmdb_id") or r.get("imdb_id")
            if k in seen:
                r["_orig_lang"] = seen[k]
                continue
            v = await _original_language(c, r.get("imdb_id"), r.get("tmdb_id"), r.get("type"))
            seen[k] = v
            r["_orig_lang"] = v


async def _tvdb_to_imdb(tvdbid: str) -> Optional[str]:
    if not TMDB_API_KEY or not tvdbid:
        return None
    if tvdbid in _tvdb_cache:
        return _tvdb_cache[tvdbid]
    imdb = None
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(
                f"https://api.themoviedb.org/3/find/{tvdbid}",
                params={"api_key": TMDB_API_KEY, "external_source": "tvdb_id"},
            )
            data = r.json()
            results = data.get("tv_results") or data.get("movie_results") or []
            if results:
                tmdb_id = results[0]["id"]
                media = "tv" if data.get("tv_results") else "movie"
                r2 = await c.get(
                    f"https://api.themoviedb.org/3/{media}/{tmdb_id}/external_ids",
                    params={"api_key": TMDB_API_KEY},
                )
                imdb = (r2.json() or {}).get("imdb_id") or None
    except Exception:
        imdb = None
    _tvdb_cache[tvdbid] = imdb
    return imdb


# --------------------------------------------------------------------------- #
# import list  (Radarr / Sonarr "Custom List")
# --------------------------------------------------------------------------- #
async def _tmdb_to_tvdb(session, tmdb_id) -> Optional[int]:
    if not TMDB_API_KEY or not tmdb_id:
        return None
    if tmdb_id in _tmdb_tvdb_cache:
        return _tmdb_tvdb_cache[tmdb_id]
    tvdb = None
    try:
        r = await session.get(
            f"https://api.themoviedb.org/3/tv/{tmdb_id}/external_ids",
            params={"api_key": TMDB_API_KEY},
        )
        if r.status_code == 200:
            v = (r.json() or {}).get("tvdb_id")
            tvdb = int(v) if v else None
    except Exception:
        tvdb = None
    _tmdb_tvdb_cache[tmdb_id] = tvdb
    return tvdb


async def _list_titles(media_type, days, min_seeders, languages, resolutions, limit):
    where = ["ti.type = $1", "ti.tmdb_id IS NOT NULL"]
    args: list = [media_type]
    if days and days > 0:
        args.append(int(days) * 86400)
        where.append(f"ti.created_at > extract(epoch from now()) - ${len(args)}::bigint")
    if languages:
        args.append(languages)
        where.append(f"ti.languages && ${len(args)}::varchar[]")

    having = ["max(ti.seeders) >= $%d" % (len(args) + 1)]
    args.append(int(min_seeders))
    if resolutions:
        args.append(resolutions)
        having.append(
            f"bool_or(lower(ti.parsed_data->>'resolution') = ANY(${len(args)}::text[]))"
        )

    args.append(min(max(int(limit), 1), LIST_HARD_LIMIT))
    sql = f"""
        SELECT ti.tmdb_id,
               (array_agg(ti.imdb_id) FILTER (WHERE ti.imdb_id IS NOT NULL))[1] AS imdb_id,
               (array_agg(ti.parsed_data->>'parsed_title'
                          ORDER BY length(ti.parsed_data->>'parsed_title')))[1] AS title,
               max(ti.seeders) AS seeders,
               max(ti.created_at) AS last_added
        FROM torrent_items ti
        WHERE {' AND '.join(where)}
        GROUP BY ti.tmdb_id
        HAVING {' AND '.join(having)}
        ORDER BY max(ti.created_at) DESC
        LIMIT ${len(args)}
    """
    async with pool.acquire() as con:
        return [dict(r) for r in await con.fetch(sql, *args)]


def _list_params(p):
    def _i(name, default):
        try:
            return int(p.get(name)) if p.get(name) not in (None, "") else default
        except ValueError:
            return default

    langs = p.get("languages")
    langs = [x.strip().lower() for x in langs.split(",") if x.strip()] if langs is not None else LIST_LANGUAGES
    res = p.get("resolutions")
    res = [x.strip().lower() for x in res.split(",") if x.strip()] if res is not None else LIST_RESOLUTIONS
    return dict(
        days=_i("days", LIST_DAYS),
        min_seeders=_i("min_seeders", LIST_MIN_SEEDERS),
        languages=langs,
        resolutions=res,
        limit=_i("limit", LIST_LIMIT),
    )


# --------------------------------------------------------------------------- #
# search
# --------------------------------------------------------------------------- #
async def _search(
    t: str,
    q: str,
    imdbid: str,
    tmdbid: str,
    tvdbid: str,
    season: Optional[int],
    ep: Optional[int],
    limit: int,
):
    where = ["ti.info_hash IS NOT NULL", "btrim(ti.info_hash) <> ''"]
    args: list = []

    media_type = {"movie": "movie", "movie-search": "movie", "tvsearch": "series", "tv-search": "series"}.get(t)
    if media_type:
        args.append(media_type)
        where.append(f"ti.type = ${len(args)}")

    imdb = _norm_imdb(imdbid)
    if not imdb and tvdbid:
        imdb = await _tvdb_to_imdb(tvdbid)

    id_parts = []
    if imdb:
        args.append(imdb)
        id_parts.append(f"ti.imdb_id = ${len(args)}")
    if tmdbid and tmdbid.isdigit():
        args.append(int(tmdbid))
        id_parts.append(f"ti.tmdb_id = ${len(args)}")
    id_clause = f"({' OR '.join(id_parts)})" if id_parts else None

    if id_clause:
        where.append(id_clause)
    elif q:
        # tokenised, trigram-accelerated match (dot/space/underscore insensitive):
        # every word of the query must appear in the raw or parsed title.
        for word in re.split(r"[\s._-]+", q.strip()):
            if len(word) < 2:
                continue
            args.append(f"%{word}%")
            p = f"${len(args)}"
            where.append(f"(ti.raw_title ILIKE {p} OR (ti.parsed_data->>'parsed_title') ILIKE {p})")
    # else: no q / no id -> RSS latest

    if MIN_SEEDERS > 0:
        args.append(MIN_SEEDERS)
        where.append(f"ti.seeders >= ${len(args)}")

    if LANGUAGES:
        args.append(LANGUAGES)
        where.append(f"ti.languages && ${len(args)}::varchar[]")

    if CACHED_ONLY:
        args.append(CACHED_SERVICE)
        where.append(
            f"EXISTS (SELECT 1 FROM debrid_cache dc WHERE dc.info_hash = ti.info_hash "
            f"AND dc.service = ${len(args)} AND dc.expires_at > extract(epoch from now()))"
        )

    if media_type == "series" and season is not None:
        # Push the season match into SQL *before* the LIMIT: a low-seeder season
        # pack (e.g. a remux) otherwise gets crowded out by higher-seeded
        # releases from OTHER seasons of the same show before the python-side
        # _season_matches() ever sees it. Mirrors _season_matches()'s season logic.
        args.append(season)
        s_idx = len(args)
        args.append(rf"(?:^|[^0-9])S0*{season}(?:[^0-9]|$)")
        s_re_idx = len(args)
        args.append(rf"saison\s*0*{season}\y")
        s_fr_re_idx = len(args)
        where.append(f"""(
            (ti.parsed_data->'seasons')::jsonb @> to_jsonb(ARRAY[${s_idx}]::int[])
            OR (ti.parsed_data->'seasons') IS NULL
            OR (ti.parsed_data->'seasons')::jsonb = '[]'::jsonb
            OR ti.raw_title ~* ${s_re_idx}
            OR ti.raw_title ~* ${s_fr_re_idx}
            OR ti.raw_title ~* '\\yint(e|é)grale\\y|\\ycomplete\\y|\\yfull\\y'
        )""")

    order = "ti.created_at DESC" if (not id_clause and not q) else "ti.seeders DESC NULLS LAST, ti.size DESC"
    fetch = min(max(limit, 1), HARD_LIMIT)
    # small over-fetch margin: SQL now already scopes to the right season, this
    # only absorbs the remaining episode-level python filtering below
    sql_fetch = fetch * 2 if (media_type == "series" and season is not None) else fetch

    sql = f"""
        SELECT ti.id, ti.raw_title, ti.size, ti.info_hash, ti.magnet, ti.link,
               ti.seeders, ti.languages, ti.indexer, ti.type, ti.imdb_id, ti.tmdb_id,
               ti.created_at, ti.parsed_data
        FROM torrent_items ti
        WHERE {' AND '.join(where)}
        ORDER BY {order}
        LIMIT {sql_fetch}
    """
    async with pool.acquire() as con:
        rows = [dict(r) for r in await con.fetch(sql, *args)]

    if media_type == "series" and season is not None:
        rows = [r for r in rows if _season_matches(r, season, ep)]

    rows = rows[:fetch]
    await _attach_original_languages(rows)
    return rows


def _season_matches(row, season: int, ep: Optional[int]) -> bool:
    pd = row.get("parsed_data") or {}
    if isinstance(pd, str):
        try:
            import json
            pd = json.loads(pd)
        except Exception:
            pd = {}
    seasons = pd.get("seasons") or []
    episodes = pd.get("episodes") or []
    title = row.get("raw_title") or ""

    # explicit season match, or full-series / season-less pack (let *arr decide),
    # or the title mentions the season
    ok_season = (
        season in seasons
        or not seasons
        or bool(re.search(rf"(?:^|[^0-9])S0*{season}(?:[^0-9]|$)", title, re.I))
        or bool(re.search(rf"saison\s*0*{season}\b", title, re.I))
        or bool(re.search(rf"\b(?:int[eé]grale|complete|full)\b", title, re.I))
    )
    if not ok_season:
        return False
    if ep is None:
        return True
    return (ep in episodes) or (not episodes) or bool(
        re.search(rf"S0*{season}E0*{ep}(?:[^0-9]|$)", title, re.I)
    )


# --------------------------------------------------------------------------- #
# XML rendering
# --------------------------------------------------------------------------- #
def _caps_xml() -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="1.1" title="{html.escape(TITLE)}" />
  <limits max="{HARD_LIMIT}" default="{DEFAULT_LIMIT}" />
  <retention days="9999" />
  <searching>
    <search available="yes" supportedParams="q" />
    <tv-search available="yes" supportedParams="q,season,ep,imdbid,tvdbid,tmdbid" />
    <movie-search available="yes" supportedParams="q,imdbid,tmdbid" />
    <audio-search available="no" supportedParams="q" />
    <book-search available="no" supportedParams="q" />
  </searching>
  <categories>
    <category id="2000" name="Movies">
      <subcat id="2040" name="Movies/HD" />
      <subcat id="2010" name="Movies/Foreign" />
    </category>
    <category id="5000" name="TV">
      <subcat id="5040" name="TV/HD" />
      <subcat id="5020" name="TV/Foreign" />
    </category>
  </categories>
</caps>"""


def _item_xml(row) -> str:
    magnet = _magnet(row)
    if not magnet:
        return ""
    ih = (row["info_hash"] or "").strip().lower()
    size = int(row["size"] or 0)
    seeders = int(row["seeders"] or 0)
    cat = _category(row["type"])
    title = html.escape(row["raw_title"] or ih)
    pub = formatdate(float(row["created_at"] or time.time()), usegmt=True)
    indexer = html.escape(row.get("indexer") or "reborn")
    m_attr = html.escape(magnet, quote=True)

    attrs = [
        ("category", cat),
        ("category", "2040" if cat == "2000" else "5040"),
        ("seeders", str(seeders)),
        ("peers", str(seeders + 1)),
        ("size", str(size)),
        ("infohash", ih),
        ("magneturl", magnet),
        ("downloadvolumefactor", "0"),
        ("uploadvolumefactor", "1"),
    ]
    for lang in _languages(row, row.get("_orig_lang")):
        attrs.append(("language", lang))
    if row.get("imdb_id"):
        attrs.append(("imdbid", str(row["imdb_id"])))
    if row.get("tmdb_id"):
        attrs.append(("tmdbid", str(row["tmdb_id"])))
    attr_xml = "\n      ".join(
        f'<torznab:attr name="{k}" value="{html.escape(str(v), quote=True)}" />' for k, v in attrs
    )
    return f"""    <item>
      <title>{title}</title>
      <guid isPermaLink="false">{ih}</guid>
      <jackettindexer id="reborn">{indexer}</jackettindexer>
      <type>public</type>
      <comments></comments>
      <pubDate>{pub}</pubDate>
      <size>{size}</size>
      <link>{m_attr}</link>
      <enclosure url="{m_attr}" length="{size}" type="application/x-bittorrent" />
      {attr_xml}
    </item>"""


def _feed_xml(rows) -> str:
    items = "\n".join(x for x in (_item_xml(r) for r in rows) if x)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:torznab="http://torznab.com/schemas/2015/feed">
  <channel>
    <atom:link href="" rel="self" type="application/rss+xml" />
    <title>{html.escape(TITLE)}</title>
    <description>stream-fusion-reborn database as a Torznab feed</description>
    <link>https://github.com/</link>
    <language>fr-FR</language>
    <category>search</category>
{items}
  </channel>
</rss>"""


def _err_xml(code: int, desc: str) -> Response:
    body = f'<?xml version="1.0" encoding="UTF-8"?>\n<error code="{code}" description="{html.escape(desc)}" />'
    return Response(content=body, media_type="application/xml", status_code=200)


# --------------------------------------------------------------------------- #
# routes
# --------------------------------------------------------------------------- #
@app.get("/", response_class=PlainTextResponse)
async def root():
    return "reborn-torznab ok — Torznab endpoint at /api"


@app.get("/health", response_class=PlainTextResponse)
async def health():
    try:
        async with pool.acquire() as con:
            n = await con.fetchval("SELECT count(*) FROM torrent_items")
        warm = "on" if (WARM_CACHE and STREMIO_CONFIG) else ("no-config" if WARM_CACHE else "off")
        sync = "on" if (STREMIO_SYNC and STREMIO_CONFIG) else ("no-config" if STREMIO_SYNC else "off")
        return (f"ok rows={n} warm={warm} warm_seen={len(_warm_seen)} "
                f"sync={sync} sync_cache={len(_sync_cache)} uncached_fallback={STREMIO_SYNC_UNCACHED}")
    except Exception as e:  # noqa
        return PlainTextResponse(f"db error: {e}", status_code=503)


@app.get("/list")
async def list_help(request: Request):
    prm = _list_params(request.query_params)
    return {
        "usage": {
            "radarr": "/list/radarr  (Radarr → Import Lists → Custom Lists → URL)",
            "sonarr": "/list/sonarr  (Sonarr → Import Lists → Custom List → URL)",
        },
        "query_params": {
            "days": "release added in the last N days (0 = all-time)",
            "min_seeders": "keep a title only if one of its releases has >= this many seeders",
            "languages": "comma list of stream-fusion tags (fr,multi,vostfr,en,…); empty = any",
            "resolutions": "comma list e.g. 2160p,1080p; empty = any",
            "limit": "max titles",
        },
        "effective": prm,
        "defaults_env": {
            "LIST_DAYS": LIST_DAYS, "LIST_MIN_SEEDERS": LIST_MIN_SEEDERS,
            "LIST_LANGUAGES": LIST_LANGUAGES, "LIST_RESOLUTIONS": LIST_RESOLUTIONS,
            "LIST_LIMIT": LIST_LIMIT,
        },
    }


@app.get("/list/radarr")
async def list_radarr(request: Request):
    prm = _list_params(request.query_params)
    rows = await _list_titles("movie", **prm)
    out = []
    for r in rows:
        tid = int(r["tmdb_id"])
        item = {"tmdb_id": tid, "tmdbId": tid, "title": r.get("title") or ""}
        if r.get("imdb_id"):
            item["imdb_id"] = r["imdb_id"]
            item["imdbId"] = r["imdb_id"]
        out.append(item)
    return out


@app.get("/list/sonarr")
async def list_sonarr(request: Request):
    prm = _list_params(request.query_params)
    rows = await _list_titles("series", **prm)
    sem = asyncio.Semaphore(25)
    out = []
    async with httpx.AsyncClient(timeout=8) as c:
        async def resolve(r):
            async with sem:
                tvdb = await _tmdb_to_tvdb(c, r["tmdb_id"])
            if tvdb:
                out.append({"tvdb_id": tvdb, "tvdbId": tvdb, "title": r.get("title") or ""})

        await asyncio.gather(*(resolve(r) for r in rows))
    _save_cache()
    return out


@app.get("/api")
async def api(request: Request):
    p = request.query_params
    t = (p.get("t") or "search").lower()

    if API_KEY and t != "caps" and p.get("apikey", "") != API_KEY:
        return _err_xml(100, "Incorrect user credentials")

    if t == "caps":
        return Response(content=_caps_xml(), media_type="application/xml")

    if t not in ("search", "tvsearch", "tv-search", "movie", "movie-search"):
        return _err_xml(202, f"No such function: {t}")

    def _int(name):
        v = p.get(name)
        try:
            return int(v) if v not in (None, "") else None
        except ValueError:
            return None

    try:
        limit = int(p.get("limit") or DEFAULT_LIMIT)
    except ValueError:
        limit = DEFAULT_LIMIT

    season = _int("season")
    ep = _int("ep")
    media_type = {"tvsearch": "series", "tv-search": "series",
                  "movie": "movie", "movie-search": "movie"}.get(t)

    imdb = _norm_imdb(p.get("imdbid") or "")
    if not imdb and p.get("tvdbid"):
        imdb = await _tvdb_to_imdb(p.get("tvdbid") or "")

    # synchronous cached-only search: return ONLY the AllDebrid-cached releases
    used_sync = False
    rows = None
    if STREMIO_SYNC and STREMIO_CONFIG and media_type and imdb:
        want = (media_type == "movie") or (media_type == "series" and season is not None)
        if want:
            srows = await _stremio_rows(media_type, imdb, p.get("tmdbid"), season, ep, limit)
            if srows is not None:
                # StreamFusion answered (even with 0 cached) -> that IS the result
                rows = srows
                used_sync = True
            elif STREMIO_SYNC_FALLBACK == "empty":
                # transport failure and no DB fallback wanted
                rows = []
                used_sync = True
            # else: srows is None + fallback "db" -> fall through to _search below

    # cache-warming, now AWAITED (synchronous) instead of fire-and-forget: hit
    # StreamFusion for this exact title/season BEFORE querying our own DB, so a
    # never-checked title gets verified within THIS request instead of only
    # showing up on a second search a few seconds later once the background
    # task had time to finish. Still deduped/skipped within WARM_TTL, so a
    # recently-warmed title adds no extra latency.
    if WARM_CACHE and STREMIO_CONFIG and not used_sync:
        if imdb:
            if media_type == "series" and season is not None:
                sid = f"{imdb}:{season}:{ep if ep is not None else 1}"
                await _warm_stream("series", sid)
            elif media_type == "movie" or t == "search":
                await _warm_stream("movie", imdb)
        elif media_type == "movie" and (p.get("tmdbid") or "").isdigit():
            # no IMDb id known (e.g. not yet assigned upstream) -> StreamFusion
            # also accepts a "tmdb:<id>" stream id for movies, use that instead
            await _warm_stream("movie", f"tmdb:{p.get('tmdbid')}")

    if rows is None:
        rows = await _search(
            t=t,
            q=p.get("q") or "",
            imdbid=p.get("imdbid") or "",
            tmdbid=p.get("tmdbid") or "",
            tvdbid=p.get("tvdbid") or "",
            season=season,
            ep=ep,
            limit=limit,
        )

    return Response(content=_feed_xml(rows), media_type="application/rss+xml")
