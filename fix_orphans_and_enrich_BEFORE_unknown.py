# backend/fix_orphans_and_enrich.py
import os, re, time, json, sqlite3, requests
from pathlib import Path
from typing import Dict, Any, List, Optional

# ADD: single-ID mode support
import sys

# --- Config (env + defaults) ---
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ.get("RAWG_API_KEY", "").strip()
DB      = os.environ.get("LG_DB", "latestgames.db").strip()
ROOT    = Path(os.environ.get("LG_SHOTS_DIR", "screenshots")).resolve()  # screenshots/<id>/*
RAWG    = "https://api.rawg.io/api"
TIMEOUT = 30
RETRY_429_SLEEP = 60

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
    cols = {r["name"] for r in cur.fetchall()}
    add_cols = []
    if "description" not in cols:   add_cols.append("ALTER TABLE games ADD COLUMN description TEXT")
    if "website" not in cols:       add_cols.append("ALTER TABLE games ADD COLUMN website TEXT")
    if "age_rating" not in cols:    add_cols.append("ALTER TABLE games ADD COLUMN age_rating TEXT")
    if "cover_image" not in cols:   add_cols.append("ALTER TABLE games ADD COLUMN cover_image TEXT")
    if add_cols:
        for sql in add_cols: cur.execute(sql)

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

    # ADD: core lookup + link tables for genres/platforms (idempotent)
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
    """)
    cur.executescript("""
    CREATE INDEX IF NOT EXISTS idx_game_genres_gid ON game_genres(game_id);
    CREATE INDEX IF NOT EXISTS idx_game_platforms_gid ON game_platforms(game_id);
    """)
    conn.commit()

# --- RAWG helpers ---
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

MEDIA_PREFIX = "https://media.rawg.io/media/"
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

def fetch_series(game_id: int) -> List[Dict[str, Any]]:
    j = _rawg_get(f"{RAWG}/games/{game_id}/game-series", {}) or {}
    return j.get("results", []) or []

def fetch_additions(game_id: int) -> List[Dict[str, Any]]:
    j = _rawg_get(f"{RAWG}/games/{game_id}/additions", {}) or {}
    return j.get("results", []) or []

def fetch_screenshots_api(game_id: int) -> List[str]:
    j = _rawg_get(f"{RAWG}/games/{game_id}/screenshots", {}) or {}
    arr = j.get("results", []) or []
    return [normalize_rawg_image(x.get("image")) for x in arr if x.get("image")]

# --- DB ops ---
def upsert_game_core(conn: sqlite3.Connection, details: Dict[str, Any]):
    gid = details["id"]
    slug = details.get("slug") or str(gid)
    name = details.get("name") or slug
    released = details.get("released") or None
    rating = details.get("rating") or None

    cur = conn.cursor()
    # insert core (ignore if exists)
    cur.execute("""
    INSERT OR IGNORE INTO games (id, slug, name, released, rating)
    VALUES (?, ?, ?, ?, ?)
    """, (gid, slug, name, released, rating))

    # update enrichable fields
    about = details.get("description_raw") or None
    website = details.get("website") or None
    esrb = details.get("esrb_rating") or {}
    age = esrb.get("name") or None

    cover = choose_cover_from(details)  # normalized

    cur.execute("""
    UPDATE games
       SET description = COALESCE(?, description),
           website     = COALESCE(?, website),
           age_rating  = COALESCE(?, age_rating),
           cover_image = COALESCE(?, cover_image),
           released    = COALESCE(?, released),
           rating      = COALESCE(?, rating),
           slug        = COALESCE(?, slug),
           name        = COALESCE(?, name)
     WHERE id = ?
    """, (about, website, age, cover, released, rating, slug, name, gid))
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

# ADD: genres/platforms from RAWG details
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

def upsert_links(conn: sqlite3.Connection, gid: int):
    cur = conn.cursor()
    # series
    try:
        series = fetch_series(gid)
        if series:
            rows = []
            for s in series:
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
        adds = fetch_additions(gid)
        if adds:
            rows = []
            for a in adds:
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

def cover_fallback_from_screens(conn: sqlite3.Connection, gid: int):
    """If games.cover_image is empty, set it from RAWG screenshots first result."""
    cur = conn.cursor()
    cur.execute("SELECT cover_image FROM games WHERE id = ?", (gid,))
    row = cur.fetchone()
    if not row or (row["cover_image"] or "").strip():
        return
    # try API screenshots
    urls = fetch_screenshots_api(gid)
    if urls:
        cur.execute("UPDATE games SET cover_image = ? WHERE id = ?", (urls[0], gid))
        conn.commit()

def store_screenshots(conn: sqlite3.Connection, gid: int):
    """Ensure screenshots table has at least the first RAWG screenshot (if empty)."""
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM screenshots WHERE game_id = ? LIMIT 1", (gid,))
    if cur.fetchone():
        return
    urls = fetch_screenshots_api(gid)
    if urls:
        cur.executemany(
            "INSERT INTO screenshots (game_id, url) VALUES (?, ?)",
            [(gid, u) for u in urls[:40]]
        )
        conn.commit()

# --- Enrichment controller ---
def needs_enrich(conn: sqlite3.Connection, gid: int) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT
          COALESCE(description,'') = '' AS no_about,
          COALESCE(website,'')     = '' AS no_site,
          COALESCE(age_rating,'')  = '' AS no_age,
          COALESCE(cover_image,'') = '' AS no_cover
          released IS NULL         AS no_released,
          rating IS NULL           AS no_rating
        FROM games WHERE id = ?
    """, (gid,))
    row = cur.fetchone()
    if not row:
        return True
    no_about, no_site, no_age, no_cover, no_released, no_rating = [bool(x) for x in row]
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

    # ADD: also check genres/platforms
    cur.execute("SELECT 1 FROM game_genres WHERE game_id = ? LIMIT 1", (gid,))
    no_genres = cur.fetchone() is None
    cur.execute("SELECT 1 FROM game_platforms WHERE game_id = ? LIMIT 1", (gid,))
    no_platforms = cur.fetchone() is None

    return any([no_about, no_site, no_age, no_cover, no_released, no_rating, no_devs, no_pubs, no_tags, no_series, no_adds, no_genres, no_platforms])

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

    upsert_game_core(conn, details)
    upsert_lists(conn, gid, details)
    upsert_genres_platforms(conn, gid, details)  # ADD: fill genres & platforms
    upsert_links(conn, gid)
    cover_fallback_from_screens(conn, gid)  # only if still empty
    store_screenshots(conn, gid)
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
    return {r["id"] for r in conn.execute("SELECT id FROM games")}

# --- Main ---
def main():
    # ADD: single-ID (or list) mode: python fix_orphans_and_enrich.py 27 123,456
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
                    "SELECT cover_image, description, website, age_rating FROM games WHERE id=?",
                    (gid,)
                ).fetchone()
                print(f"[{gid}] enriched. cover={bool(row['cover_image'])}, about={bool(row['description'])}, site={bool(row['website'])}, age={bool(row['age_rating'])}")
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
    all_ids = [r["id"] for r in cur.fetchall()]
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
