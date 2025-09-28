# backend/fix_orphans_and_enrich.py
# Adds since last version:
#  - Metascore fields (number + color) alongside existing RAWG user rating.
#  - Full screenshots pagination (reuse once-fetched data for cover + storage).
#  - Movies (video) metadata via /games/{id}/movies into a normalized `media` table.
#  - Backward-compatible population of legacy `screenshots` table (first page used to fill).
#  - "Where to buy" store links from RAWG game details into normalized `stores` and `game_stores` tables.
#  - NEW: "Games like <X>" suggestions (up to 8) stored for each game (name, image, platforms, metascore, released, genres).
#
# Notes:
#  - No downloads of images/videos; we store CDN URLs (and video preview thumbs) only.
#  - Soft cap MAX_IMAGES to keep request costs bounded; ALL videos are saved.
#  - Existing behavior retained unless expanded (no frontend-breaking changes).
#  - No downloads of images/videos; we store CDN URLs (and video preview thumbs) only.
#  - Soft cap MAX_IMAGES to keep request costs bounded; ALL videos are saved.
#  - Existing behavior retained unless expanded (no frontend-breaking changes).

import os, time, sqlite3, requests
from pathlib import Path
from typing import Dict, Any, List, Optional
import sys

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ.get("RAWG_API_KEY", "").strip()
DB      = os.environ.get("LG_DB", "latestgames.db").strip()
ROOT    = Path(os.environ.get("LG_SHOTS_DIR", "screenshots")).resolve()
RAWG    = "https://api.rawg.io/api"
TIMEOUT = 30
RETRY_429_SLEEP = 60

# Soft quota for images (you can raise later)
MAX_IMAGES = int(os.environ.get("LG_MAX_IMAGES", 50))

if not API_KEY:
    raise SystemExit("RAWG_API_KEY missing. Add it to backend/.env or environment.")

# --- SQLite helpers ---

def get_conn():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    # columns (safe if already exist)
    cur.execute("PRAGMA table_info(games)")
    cols = {r[1] for r in cur.fetchall()}  # (cid, name, ...)
    add_cols = []
    if "description" not in cols:       add_cols.append("ALTER TABLE games ADD COLUMN description TEXT")
    if "website" not in cols:           add_cols.append("ALTER TABLE games ADD COLUMN website TEXT")
    if "age_rating" not in cols:        add_cols.append("ALTER TABLE games ADD COLUMN age_rating TEXT")
    if "cover_image" not in cols:       add_cols.append("ALTER TABLE games ADD COLUMN cover_image TEXT")
    # NEW: metascore columns
    if "metascore_number" not in cols:  add_cols.append("ALTER TABLE games ADD COLUMN metascore_number INTEGER")
    if "metascore_color"  not in cols:  add_cols.append("ALTER TABLE games ADD COLUMN metascore_color TEXT")

    for sql in add_cols:
        cur.execute(sql)

    # aux tables (idempotent)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS game_developers (
        game_id INTEGER NOT NULL,
        developer TEXT NOT NULL,
        PRIMARY KEY (game_id, developer)
    );
    CREATE TABLE IF NOT EXISTS game_publishers (
        game_id INTEGER NOT NULL,
        publisher TEXT NOT NULL,
        PRIMARY KEY (game_id, publisher)
    );
    CREATE TABLE IF NOT EXISTS game_tags (
        game_id INTEGER NOT NULL,
        tag TEXT NOT NULL,
        PRIMARY KEY (game_id, tag)
    );
    CREATE TABLE IF NOT EXISTS game_series_links (
        game_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        url TEXT NOT NULL,
        PRIMARY KEY (game_id, name, url)
    );
    CREATE TABLE IF NOT EXISTS game_additions_links (
        game_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        url TEXT NOT NULL,
        PRIMARY KEY (game_id, name, url)
    );
    -- Legacy screenshots table retained for compatibility
    CREATE TABLE IF NOT EXISTS screenshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        url TEXT NOT NULL
    );
    """)

    # helpful indexes (safe)
    cur.executescript("""
    CREATE INDEX IF NOT EXISTS idx_games_slug ON games(slug);
    CREATE INDEX IF NOT EXISTS idx_games_released ON games(released);
    CREATE INDEX IF NOT EXISTS idx_dev_gid ON game_developers(game_id);
    CREATE INDEX IF NOT EXISTS idx_pub_gid ON game_publishers(game_id);
    CREATE INDEX IF NOT EXISTS idx_tag_gid ON game_tags(game_id);
    CREATE INDEX IF NOT EXISTS idx_series_gid ON game_series_links(game_id);
    CREATE INDEX IF NOT EXISTS idx_additions_gid ON game_additions_links(game_id);
    CREATE INDEX IF NOT EXISTS idx_shots_gid ON screenshots(game_id);
    """)

    # Core lookup + link tables for genres/platforms (idempotent)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS genres (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS game_genres (
        game_id INTEGER NOT NULL,
        genre_id INTEGER NOT NULL,
        PRIMARY KEY (game_id, genre_id)
    );
    CREATE TABLE IF NOT EXISTS platforms (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS game_platforms (
        game_id INTEGER NOT NULL,
        platform_id INTEGER NOT NULL,
        PRIMARY KEY (game_id, platform_id)
    );
    CREATE INDEX IF NOT EXISTS idx_game_genres_gid ON game_genres(game_id);
    CREATE INDEX IF NOT EXISTS idx_game_platforms_gid ON game_platforms(game_id);
    """)

    # NEW: normalized media table (images & videos)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS media (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        type TEXT NOT NULL CHECK(type IN ('image','video')),
        url TEXT NOT NULL,
        preview_url TEXT,
        position INTEGER,
        UNIQUE(game_id, type, url)
    );
    CREATE INDEX IF NOT EXISTS idx_media_gid ON media(game_id);
    CREATE INDEX IF NOT EXISTS idx_media_gid_type ON media(game_id, type);
    """)

    # NEW: stores (where to buy) and link table
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS stores (
        id INTEGER PRIMARY KEY,
        name TEXT,
        slug TEXT,
        domain TEXT,
        logo_url TEXT,      -- optional, to be filled later
        hover_image_url TEXT -- optional, to be filled later
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stores_slug ON stores(slug);

    CREATE TABLE IF NOT EXISTS game_stores (
        game_id INTEGER NOT NULL,
        store_id INTEGER NOT NULL,
        url TEXT,
        PRIMARY KEY (game_id, store_id)
    );
    CREATE INDEX IF NOT EXISTS idx_game_stores_gid ON game_stores(game_id);
    """)

    conn.commit()

    # NEW: suggestions tables (denormalized for quick display)
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS game_suggestions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,           -- source game
        position INTEGER NOT NULL,          -- 1..8 as seen on RAWG page
        suggested_id INTEGER,               -- RAWG id if present
        name TEXT,
        image_url TEXT,
        platforms_csv TEXT,                 -- e.g., "PC; PlayStation 4"
        metascore_number INTEGER,
        metascore_color TEXT,
        released TEXT,                      -- YYYY-MM-DD
        genres_csv TEXT,
        UNIQUE(game_id, position)
    );
    CREATE INDEX IF NOT EXISTS idx_suggestions_gid ON game_suggestions(game_id);
    """)

    conn.commit()

# --- RAWG helpers ---

MEDIA_PREFIX = "https://media.rawg.io/media/"

def _rawg_get(url: str, params: Dict[str, Any], max_retries=3) -> Optional[Dict[str, Any]]:
    p = dict(params)
    p["key"] = API_KEY
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, params=p, timeout=TIMEOUT)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                return None
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            time.sleep(RETRY_429_SLEEP)
            continue
        if r.status_code in (500, 502, 503, 504):
            time.sleep(2 * attempt)
            continue
        r.raise_for_status()
    return None


def normalize_rawg_image(url: Optional[str]) -> Optional[str]:
    """Strip /crop/*/*/ or /resize/*/-/ → keep /media/games/<hash>.jpg"""
    if not url or MEDIA_PREFIX not in url:
        return url
    tail = url.split("/media/", 1)[1]
    parts = tail.split("/")
    if parts and parts[0] in ("crop", "resize"):
        try:
            gi = parts.index("games")
            tail = "/".join(parts[gi:])
        except ValueError:
            tail = "/".join(parts)
    return MEDIA_PREFIX + tail


def choose_cover_from(details: Dict[str, Any]) -> Optional[str]:
    for k in ("background_image", "background_image_additional"):
        v = details.get(k)
        if v:
            return normalize_rawg_image(v)
    for s in details.get("short_screenshots") or []:
        img = (s or {}).get("image")
        if img:
            return normalize_rawg_image(img)
    return None


def fetch_details(game_id: int) -> Optional[Dict[str, Any]]:
    return _rawg_get(f"{RAWG}/games/{game_id}", {})


def fetch_all_screenshots(game_id: int, max_images: int = MAX_IMAGES) -> List[Dict[str, Any]]:
    """Return list of screenshot dicts from RAWG with 'image', maybe width/height, paginated."""
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        j = _rawg_get(f"{RAWG}/games/{game_id}/screenshots", {"page": page}) or {}
        results = j.get("results", []) or []
        for item in results:
            img = item.get("image")
            if not img:
                continue
            out.append({
                "image": normalize_rawg_image(img),
                "width": item.get("width"),
                "height": item.get("height"),
            })
            if len(out) >= max_images:
                return out
        if not j.get("next"):
            break
        page += 1
    return out


def fetch_movies(game_id: int) -> List[Dict[str, Any]]:
    """Return list of movie dicts with 'url' (max or 480) and 'preview' (thumbnail)."""
    j = _rawg_get(f"{RAWG}/games/{game_id}/movies", {}) or {}
    out: List[Dict[str, Any]] = []
    for item in j.get("results", []) or []:
        data = item.get("data") or {}
        url = data.get("max") or data.get("480")
        preview = item.get("preview")
        if url:
            out.append({
                "url": url,
                "preview": preview,
                "name": item.get("name")
            })
    return out


def fetch_suggestions(game_id: int, limit: int = 8) -> List[Dict[str, Any]]:
    """Fetch up to `limit` visually similar games. Endpoint may be business-tier; handle gracefully."""
    j = _rawg_get(f"{RAWG}/games/{game_id}/suggested", {}) or {}
    results = j.get("results", []) or []
    out: List[Dict[str, Any]] = []
    for item in results[:limit]:
        # Normalize minimal fields without extra detail calls
        out.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "image": normalize_rawg_image(item.get("background_image")),
            "released": item.get("released"),
            "metacritic": item.get("metacritic"),
            "platforms": [
                (p.get("platform") or {}).get("name") if isinstance(p, dict) else None
                for p in (item.get("platforms") or [])
            ],
            "genres": [g.get("name") for g in (item.get("genres") or []) if g.get("name")]
        })
    return out

# --- Utility ---

def metascore_color(score: Optional[int]) -> Optional[str]:
    if score is None:
        return None
    try:
        s = int(score)
    except Exception:
        return None
    if   75 <= s <= 100: return "green"
    elif 50 <= s <= 74:  return "yellow"
    elif 0  <= s <= 49:  return "red"
    return None

# --- DB ops ---

def upsert_game_core(conn: sqlite3.Connection, details: Dict[str, Any]):
    gid = details["id"]
    slug = details.get("slug") or str(gid)
    name = details.get("name") or slug
    released = details.get("released") or None
    rating = details.get("rating") or None  # RAWG user rating

    # NEW: metascore from RAWG 'metacritic'
    mscore = details.get("metacritic")
    mcolor = metascore_color(mscore)

    cur = conn.cursor()
    # insert core (ignore if exists)
    cur.execute(
        """
        INSERT OR IGNORE INTO games (id, slug, name, released, rating)
        VALUES (?, ?, ?, ?, ?)
        """,
        (gid, slug, name, released, rating)
    )

    # update enrichable fields
    about = details.get("description_raw") or None
    website = details.get("website") or None
    esrb = details.get("esrb_rating") or {}
    age = esrb.get("name") or None

    cover = choose_cover_from(details)  # normalized

    cur.execute(
        """
        UPDATE games
           SET description       = COALESCE(?, description),
               website           = COALESCE(?, website),
               age_rating        = COALESCE(?, age_rating),
               cover_image       = COALESCE(?, cover_image),
               released          = COALESCE(?, released),
               rating            = COALESCE(?, rating),
               slug              = COALESCE(?, slug),
               name              = COALESCE(?, name),
               metascore_number  = COALESCE(?, metascore_number),
               metascore_color   = COALESCE(?, metascore_color)
         WHERE id = ?
        """,
        (about, website, age, cover, released, rating, slug, name, mscore, mcolor, gid)
    )
    conn.commit()


def upsert_lists(conn: sqlite3.Connection, gid: int, details: Dict[str, Any]):
    cur = conn.cursor()
    devs = [d.get("name") for d in (details.get("developers") or []) if d.get("name")]
    pubs = [p.get("name") for p in (details.get("publishers") or []) if p.get("name")]
    tags = [t.get("name") for t in (details.get("tags") or []) if t.get("name")]

    if devs:
        cur.executemany(
            "INSERT OR IGNORE INTO game_developers (game_id, developer) VALUES (?, ?)",
            [(gid, d.strip()) for d in devs]
        )
    if pubs:
        cur.executemany(
            "INSERT OR IGNORE INTO game_publishers (game_id, publisher) VALUES (?, ?)",
            [(gid, p.strip()) for p in pubs]
        )
    if tags:
        cur.executemany(
            "INSERT OR IGNORE INTO game_tags (game_id, tag) VALUES (?, ?)",
            [(gid, t.strip()) for t in tags]
        )
    conn.commit()


def upsert_genres_platforms(conn: sqlite3.Connection, gid: int, details: Dict[str, Any]):
    cur = conn.cursor()

    # Genres appear as [{"id": 51, "name": "Indie"}, ...]
    for g in (details.get("genres") or []):
        gid_raw = g.get("id")
        gname   = g.get("name")
        if gid_raw and gname:
            cur.execute("INSERT OR IGNORE INTO genres (id, name) VALUES (?, ?)", (int(gid_raw), gname.strip()))
            cur.execute("INSERT OR IGNORE INTO game_genres (game_id, genre_id) VALUES (?, ?)", (gid, int(gid_raw)))

    # Platforms often as [{"platform": {"id": 4, "name": "PC"}}, ...]
    for p in (details.get("platforms") or []):
        plat = p.get("platform") if isinstance(p, dict) else p
        pid_raw = (plat or {}).get("id")
        pname   = (plat or {}).get("name")
        if pid_raw and pname:
            cur.execute("INSERT OR IGNORE INTO platforms (id, name) VALUES (?, ?)", (int(pid_raw), pname.strip()))
            cur.execute("INSERT OR IGNORE INTO game_platforms (game_id, platform_id) VALUES (?, ?)", (gid, int(pid_raw)))

    conn.commit()


def upsert_store_links(conn: sqlite3.Connection, gid: int, details: Dict[str, Any]):
    """Upsert RAWG 'where to buy' store links from game details."""
    cur = conn.cursor()
    stores = details.get("stores") or []
    rows_link = []
    for s in stores:
        # RAWG format: {"id": 123, "url": "https://...", "store": {"id": 1, "name": "Steam", "slug": "steam", "domain": "store.steampowered.com"}}
        url = s.get("url") or None
        store_obj = s.get("store") or {}
        sid = store_obj.get("id") or s.get("id")
        name = store_obj.get("name") or None
        slug = store_obj.get("slug") or None
        domain = store_obj.get("domain") or None
        if sid:
            cur.execute(
                "INSERT OR IGNORE INTO stores (id, name, slug, domain) VALUES (?, ?, ?, ?)",
                (int(sid), name, slug, domain)
            )
            rows_link.append((gid, int(sid), url))
    if rows_link:
        cur.executemany(
            "INSERT OR IGNORE INTO game_stores (game_id, store_id, url) VALUES (?, ?, ?)",
            rows_link
        )
    conn.commit()


def upsert_links(conn: sqlite3.Connection, gid: int):
    cur = conn.cursor()
    # series
    try:
        series = _rawg_get(f"{RAWG}/games/{gid}/game-series", {}) or {}
        results = series.get("results", []) or []
        if results:
            rows = []
            for s in results:
                n, slug = s.get("name"), s.get("slug")
                if n and slug:
                    rows.append((gid, n.strip(), f"https://rawg.io/games/{slug}".rstrip("/")))
            if rows:
                cur.executemany(
                    "INSERT OR IGNORE INTO game_series_links (game_id, name, url) VALUES (?, ?, ?)", rows
                )
    except Exception:
        pass

    # additions
    try:
        adds = _rawg_get(f"{RAWG}/games/{gid}/additions", {}) or {}
        results = adds.get("results", []) or []
        if results:
            rows = []
            for a in results:
                n, slug = a.get("name"), a.get("slug")
                if n and slug:
                    rows.append((gid, n.strip(), f"https://rawg.io/games/{slug}".rstrip("/")))
            if rows:
                cur.executemany(
                    "INSERT OR IGNORE INTO game_additions_links (game_id, name, url) VALUES (?, ?, ?)", rows
                )
    except Exception:
        pass

    conn.commit()

# --- Media storage ---

def save_images_to_disk(gid: int, images: List[Dict[str, Any]]):
    """Download screenshot JPGs to screenshots/<game_id>/, using screenshot_###.jpg naming.
    First image saved separately as cover.jpg.
    """
    dest_dir = ROOT / str(gid)
    dest_dir.mkdir(parents=True, exist_ok=True)
    for idx, item in enumerate(images, start=1):
        url = item.get("image")
        if not url:
            continue
        if idx == 1:
            fname = "cover.jpg"
        else:
            fname = f"screenshot_{idx-1:03d}.jpg"
        target = dest_dir / fname
        if target.exists():
            continue
        try:
            with requests.get(url, stream=True, timeout=TIMEOUT) as r:
                r.raise_for_status()
                with open(target, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception:
            try:
                if target.exists() and target.stat().st_size == 0:
                    target.unlink()
            except Exception:
                pass


def cover_fallback_from_images(conn: sqlite3.Connection, gid: int, images: List[Dict[str, Any]]):
    """If games.cover_image is empty, set it from first screenshot in already-fetched list."""
    cur = conn.cursor()
    cur.execute("SELECT cover_image FROM games WHERE id = ?", (gid,))
    row = cur.fetchone()
    if not row or (row[0] or "").strip():
        return
    if images:
        cur.execute("UPDATE games SET cover_image = ? WHERE id = ?", (images[0]["image"], gid))
        conn.commit()


def store_media_images(conn: sqlite3.Connection, gid: int, images: List[Dict[str, Any]]):
    if not images:
        return
    cur = conn.cursor()
    pos = 1
    rows_media = []
    rows_legacy = []
    for item in images:
        url = item.get("image")
        if not url:
            continue
        rows_media.append((gid, 'image', url, None, pos))
        # legacy screenshots table: keep filling up to 40 (historical behavior)
        if pos <= 40:
            rows_legacy.append((gid, url))
        pos += 1
    if rows_media:
        cur.executemany(
            "INSERT OR IGNORE INTO media (game_id, type, url, preview_url, position) VALUES (?, ?, ?, ?, ?)",
            rows_media
        )
    # only insert into screenshots table if it's empty (legacy behavior)
    cur.execute("SELECT 1 FROM screenshots WHERE game_id = ? LIMIT 1", (gid,))
    if cur.fetchone() is None and rows_legacy:
        cur.executemany("INSERT INTO screenshots (game_id, url) VALUES (?, ?)", rows_legacy)
    conn.commit()

    # NEW: also download images to disk under screenshots/<game_id>/
    try:
        save_images_to_disk(gid, images)
    except Exception:
        # Don't fail the run if disk write hiccups; continue gracefully
        pass


def store_media_videos(conn: sqlite3.Connection, gid: int, videos: List[Dict[str, Any]]):
    if not videos:
        return
    cur = conn.cursor()
    rows = []
    pos = 1
    for v in videos:
        url = v.get("url")
        if not url:
            continue
        rows.append((gid, 'video', url, v.get("preview"), pos))
        pos += 1
    if rows:
        cur.executemany(
            "INSERT OR IGNORE INTO media (game_id, type, url, preview_url, position) VALUES (?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()

# --- Enrichment controller ---

def store_suggestions(conn: sqlite3.Connection, gid: int, suggestions: List[Dict[str, Any]]):
    if not suggestions:
        return
    cur = conn.cursor()
    rows = []
    pos = 1
    for s in suggestions[:8]:
        platforms = [p for p in (s.get("platforms") or []) if p]
        genres = s.get("genres") or []
        meta = s.get("metacritic")
        rows.append((
            gid,
            pos,
            s.get("id"),
            s.get("name"),
            s.get("image"),
            "; ".join(platforms) if platforms else None,
            meta,
            metascore_color(meta),
            s.get("released"),
            "; ".join(genres) if genres else None
        ))
        pos += 1
    cur.executemany(
        """
        INSERT OR REPLACE INTO game_suggestions
        (game_id, position, suggested_id, name, image_url, platforms_csv, metascore_number, metascore_color, released, genres_csv)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows
    )
    conn.commit()

# --- Enrichment controller ---

def needs_enrich(conn: sqlite3.Connection, gid: int) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          COALESCE(description,'') = '' AS no_about,
          COALESCE(website,'')     = '' AS no_site,
          COALESCE(age_rating,'')  = '' AS no_age,
          COALESCE(cover_image,'') = '' AS no_cover
        FROM games WHERE id = ?
        """,
        (gid,)
    )
    row = cur.fetchone()
    if not row:
        return True
    no_about, no_site, no_age, no_cover = [bool(x) for x in row]

    cur.execute("SELECT 1 FROM game_developers WHERE game_id = ? LIMIT 1", (gid,))
    no_devs = cur.fetchone() is None
    cur.execute("SELECT 1 FROM game_publishers WHERE game_id = ? LIMIT 1", (gid,))
    no_pubs = cur.fetchone() is None
    cur.execute("SELECT 1 FROM game_tags WHERE game_id = ? LIMIT 1", (gid,))
    no_tags = cur.fetchone() is None
    cur.execute("SELECT 1 FROM game_series_links WHERE game_id = ? LIMIT 1", (gid,))
    no_series = cur.fetchone() is None
    cur.execute("SELECT 1 FROM game_additions_links WHERE game_id = ? LIMIT 1", (gid,))
    no_adds = cur.fetchone() is None

    cur.execute("SELECT 1 FROM game_genres WHERE game_id = ? LIMIT 1", (gid,))
    no_genres = cur.fetchone() is None
    cur.execute("SELECT 1 FROM game_platforms WHERE game_id = ? LIMIT 1", (gid,))
    no_platforms = cur.fetchone() is None

    cur.execute("SELECT 1 FROM game_stores WHERE game_id = ? LIMIT 1", (gid,))
    no_stores = cur.fetchone() is None

    return any([
        no_about, no_site, no_age, no_cover,
        no_devs, no_pubs, no_tags, no_series, no_adds,
        no_genres, no_platforms, no_stores
    ])


def enrich_one(conn: sqlite3.Connection, gid: int) -> bool:
    details = fetch_details(gid)
    if not details:
        return False

    # normalize any images embedded in details (background/short_screenshots)
    if "background_image" in details:
        details["background_image"] = normalize_rawg_image(details["background_image"])
    if "background_image_additional" in details:
        details["background_image_additional"] = normalize_rawg_image(details["background_image_additional"])
    if "short_screenshots" in details and isinstance(details["short_screenshots"], list):
        for s in details["short_screenshots"]:
            if isinstance(s, dict) and s.get("image"):
                s["image"] = normalize_rawg_image(s["image"])

    upsert_game_core(conn, details)                 # includes metascore fields
    upsert_lists(conn, gid, details)
    upsert_genres_platforms(conn, gid, details)
    upsert_store_links(conn, gid, details)          # where-to-buy links
    upsert_links(conn, gid)

    # Fetch ALL screenshots once (respect soft cap), reuse for cover + storage
    images = fetch_all_screenshots(gid, max_images=MAX_IMAGES)
    cover_fallback_from_images(conn, gid, images)
    store_media_images(conn, gid, images)

    # Fetch videos metadata once and store
    videos = fetch_movies(gid)
    store_media_videos(conn, gid, videos)

    # Fetch up to 8 suggestions for quick display
    try:
        sugg = fetch_suggestions(gid, limit=8)
        store_suggestions(conn, gid, sugg)
    except Exception:
        # Endpoint can be unavailable for non-business tiers; skip gracefully
        pass

    return True

# --- Utility: find IDs from folders ---

def find_folder_ids() -> List[int]:
    if not ROOT.exists():
        return []
    ids = []
    for child in ROOT.iterdir():
        if child.is_dir():
            try:
                ids.append(int(child.name))
            except ValueError:
                continue
    return sorted(ids)


def ids_in_db(conn: sqlite3.Connection) -> set:
    return {r[0] for r in conn.execute("SELECT id FROM games")}

# --- Main ---

def main():
    # Single-ID (or list) mode: python fix_orphans_and_enrich.py 27 123,456
    if len(sys.argv) > 1:
        conn = get_conn()
        ensure_schema(conn)
        ids: List[int] = []
        for arg in sys.argv[1:]:
            for tok in arg.replace(",", " ").split():
                if tok.isdigit():
                    ids.append(int(tok))
        if not ids:
            print("No numeric IDs provided.")
            conn.close()
            return
        print(f"Single-ID mode for IDs: {ids}")
        for gid in ids:
            ok = enrich_one(conn, gid)
            if ok:
                row = conn.execute(
                    "SELECT cover_image, description, website, age_rating, metascore_number, metascore_color FROM games WHERE id=?",
                    (gid,)
                ).fetchone()
                print(
                    f"[{gid}] enriched. cover={bool(row['cover_image'])}, about={bool(row['description'])}, "
                    f"site={bool(row['website'])}, age={bool(row['age_rating'])}, meta={row['metascore_number']} ({row['metascore_color']})"
                )
            else:
                print(f"[{gid}] RAWG details not found or request failed.")
            time.sleep(0.2)
        conn.close()
        return

    print(f"DB: {DB}")
    print(f"RAWG key present: {bool(API_KEY)}")
    print(f"Screenshots root: {ROOT}")

    conn = get_conn()
    ensure_schema(conn)

    folder_ids = find_folder_ids()
    db_ids = ids_in_db(conn)

    to_insert = [gid for gid in folder_ids if gid not in db_ids]
    print(f"Found {len(folder_ids)} folders, {len(db_ids)} rows in DB.")
    print(f"Need to INSERT {len(to_insert)} missing games (by ID from folder names).")

    ins_ok = ins_fail = 0
    for gid in to_insert:
        ok = enrich_one(conn, gid)
        if ok:
            ins_ok += 1
        else:
            ins_fail += 1
        time.sleep(0.2)

    # Now enrich ALL rows that still need data
    cur = conn.cursor()
    cur.execute("SELECT id FROM games ORDER BY id ASC")
    all_ids = [r[0] for r in cur.fetchall()]
    print(f"Scanning {len(all_ids)} games for missing fields...")

    en_ok = en_skip = 0
    for gid in all_ids:
        if needs_enrich(conn, gid):
            if enrich_one(conn, gid):
                en_ok += 1
            else:
                en_skip += 1
            time.sleep(0.2)
    conn.close()

    print("----- SUMMARY -----")
    print(f"Inserted from folders: ok={ins_ok} failed={ins_fail}")
    print(f"Enriched existing rows: ok={en_ok} skipped/failed={en_skip}")
    print("Done.")

if __name__ == "__main__":
    main()
