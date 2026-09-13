# reborn-torznab

A [Torznab](https://torznab.github.io/torznab-spec/) indexer that exposes a
[stream-fusion-reborn](https://github.com/) Postgres `torrent_items` table to
Prowlarr / Radarr / Sonarr, with an optional **cache-only mode** that only
ever returns releases already confirmed available on your debrid service
(e.g. AllDebrid) - so every grab is an instant symlink and your download
client never has to sit on an uncached magnet.

## Why this exists

stream-fusion-reborn already indexes a large private-tracker catalogue into
Postgres, and StreamFusion (its Stremio addon) can check debrid availability
for a given title on demand. This project bridges the two: it's a small
FastAPI service that speaks Torznab to your \*arr stack, backed directly by
that Postgres database, with an optional synchronous "warm" step that asks
StreamFusion to verify debrid availability before answering a search.

## Features

- **Torznab search** (`t=search|movie|movie-search|tvsearch|tv-search`) over
  the `torrent_items` table - by IMDb/TMDB/TVDB id, or free-text query.
- **Cache-only filtering** (`TORZNAB_CACHED_ONLY`): joins against a
  `debrid_cache` table so only confirmed-cached releases are ever returned.
- **Synchronous cache warming** (`WARM_CACHE`): on an ID-based search, awaits
  a call to StreamFusion's `/stream` endpoint so it can verify debrid
  availability for that exact title/season and write the result to
  `debrid_cache` *before* the search responds - a title that's never been
  checked before gets verified within the same request instead of only
  showing up on a second search a few seconds later.
- **Season-pack aware**: season/episode matching happens in SQL before the
  result `LIMIT` is applied, so a low-seeder season-pack release isn't
  silently crowded out by higher-seeded releases from *other* seasons of the
  same show.
- **Language tagging**: emits `torznab:attr name="language"` per release
  (French/MULTi/original-language detection), useful if you score releases
  with TRaSH-style custom formats.
- **Import lists** (`/list/radarr`, `/list/sonarr`): StevenLu-format /
  custom-list feeds so Radarr/Sonarr can browse (add as unmonitored, or
  monitored with search-on-add disabled) the whole catalogue as a discovery
  feed, independent of the cache-only search filter.
- **`/health`**: row count + warm/sync status, for container healthchecks.

## Architecture

```
Prowlarr/Radarr/Sonarr --Torznab--> reborn-torznab --SQL--> Postgres (torrent_items, debrid_cache)
                                          |
                                          `--HTTP--> StreamFusion (/stream/...)  [cache warming]
```

reborn-torznab never talks to your debrid provider or download client
directly - it only reads/writes the shared Postgres database and calls
StreamFusion's own API for cache verification. Your existing
Prowlarr -> Radarr/Sonarr -> download client wiring is untouched.

## Requirements

- A running stream-fusion-reborn stack (Postgres with `torrent_items` +
  `debrid_cache` tables, and StreamFusion reachable over HTTP).
- Python 3.12 (via the provided Docker image) or Docker/Docker Compose.

## Configuration

Copy `.env.example` to `.env` and fill in your own values - **no credentials
ship in this repository**, the app refuses to start without `PG_DSN` set.
Every setting is documented inline in `.env.example`.

## Running it

```bash
cp .env.example .env
$EDITOR .env   # fill in PG_DSN, STREMIO_CONFIG (or STREMIO_CONFIG_FILE), TMDB_API_KEY...
docker compose up -d --build
```

The API listens on `:9118`.

## Prebuilt image

A multi-arch (`linux/amd64`, `linux/arm64`) image is built and published to
GHCR on every push to `main` and on `v*` tags:

```bash
docker pull ghcr.io/<owner>/torznab-reborn:latest
```

In `docker-compose.yml`, replace `build: .` with
`image: ghcr.io/<owner>/torznab-reborn:latest` to use it.

## Registering in Prowlarr

- Indexer type: **Generic Torznab**
- URL: `http://reborn-torznab:9118` (or `http://<host>:9118` if not on the
  same docker network)
- API path: `/api`
- API key: whatever you set as `TORZNAB_API_KEY` (or leave blank if unset)

Prowlarr will sync it to Radarr/Sonarr like any other indexer.

## Registering the import lists (optional)

- **Radarr**: Settings > Lists > add "StevenLu List", URL
  `http://reborn-torznab:9118/list/radarr`
- **Sonarr**: Settings > Lists > add "Custom List", URL
  `http://reborn-torznab:9118/list/sonarr`

Both accept query overrides: `?limit=&days=&min_seeders=&languages=&resolutions=`.

## Known limitations

- **Debrid cache is a snapshot, not live truth.** `debrid_cache` reflects
  the last time StreamFusion checked a given hash; the debrid provider's own
  cache isn't permanent and can evict a title before our recorded
  expiry. A release that was cached when returned can occasionally fail as
  "not cached" by the time your download client actually grabs it - this is
  a fundamental limitation of any caching layer sitting in front of a
  third-party cache, not something this indexer can fully eliminate.
- **Uncached magnets from private trackers with no public announce will
  never complete** on most debrid services. If your catalogue is
  private-tracker-only, keep `TORZNAB_CACHED_ONLY=true` and your download
  client's "download uncached" option off.
- `*arr` apps' own import-list sync (not this project) can be fragile against
  transient metadata-provider errors (e.g. Sonarr's Skyhook) - that's a
  Radarr/Sonarr-side behavior, unrelated to this indexer.

## License

MIT
