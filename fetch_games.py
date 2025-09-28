import os, time, json, sqlite3, requests
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv

# Load .env from this folder if present
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

API_KEY = os.environ.get("RAWG_API_KEY", "YOUR_RAWG_KEY_HERE")
BASE = "https://api.rawg.io/api"
DB = os.environ.get("LG_DB", "latestgames.db")

def http_get(url: str, params: Dict[str,Any]=None, retries:int=3, backoff:float=1.5) -> Optional[Dict[str,Any]]:
    params = params or {}
    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429,500,502,503,504):
                time.sleep(backoff*attempt)
                continue
            r.raise_for_status()
        except Exception:
            time.sleep(backoff*attempt)
    return None

def normalize_rawg_image(url: Optional[str]) -> Optional[str]:
    if not url or "/media/" not in url:
        return url
    tail = url.split("/media/",1)[1]
    parts = tail.split("/")
    if parts and parts[0] in ("crop","resize"):
        if "games" in parts:
            gi = parts.index("games")
            parts = parts[gi:]
    return "https://media.rawg.io/media/" + "/".join(parts)

def ensure_db(conn: sqlite3.Connection):
    from db_prepare import main as _prep
    _prep()

def upsert_game(conn, g):
    conn.execute("""INSERT OR IGNORE INTO games(id, slug, name, description, released, rating)
                    VALUES(?,?,?,?,?,?)""",
                 (g["id"], g.get("slug"), g.get("name"), g.get("description_raw") or "", g.get("released"), g.get("rating")))

def fetch_detail(gid:int) -> Optional[Dict[str,Any]]:
    return http_get(f"{BASE}/games/{gid}", {"key": API_KEY})

def fetch_series(gid:int) -> List[Dict[str,Any]]:
    data = http_get(f"{BASE}/games/{gid}/game-series", {"key": API_KEY}) or {}
    return data.get("results",[]) or []

def fetch_additions(gid:int) -> List[Dict[str,Any]]:
    data = http_get(f"{BASE}/games/{gid}/additions", {"key": API_KEY}) or {}
    return data.get("results",[]) or []

def fetch_screenshots(gid:int) -> List[Dict[str,Any]]:
    data = http_get(f"{BASE}/games/{gid}/screenshots", {"key": API_KEY}) or {}
    return data.get("results",[]) or []

def complete_enough(conn, gid:int) -> bool:
    c = conn.cursor()
    about = c.execute("SELECT description FROM games WHERE id=?", (gid,)).fetchone()
    cover = c.execute("SELECT cover_image FROM games WHERE id=?", (gid,)).fetchone()
    gs = c.execute("SELECT COUNT(1) FROM game_genres WHERE game_id=?", (gid,)).fetchone()
    ps = c.execute("SELECT COUNT(1) FROM game_platforms WHERE game_id=?", (gid,)).fetchone()
    pubs = c.execute("SELECT COUNT(1) FROM game_publishers WHERE game_id=?", (gid,)).fetchone()
    tags = c.execute("SELECT COUNT(1) FROM game_tags WHERE game_id=?", (gid,)).fetchone()
    conds = [bool(about and about[0]), bool(cover and cover[0]), (gs and gs[0]>0), (ps and ps[0]>0), ((pubs and pubs[0]>0) or (tags and tags[0]>0))]
    return all(conds)

def enrich(conn, gid:int, listing:Dict[str,Any]):
    d = fetch_detail(gid)
    if not d:
        return
    # about
    about = (d.get("description_raw") or "").strip()
    if about:
        conn.execute("UPDATE games SET description=? WHERE id=?", (about, gid))
    # website & age
    if d.get("website"):
        conn.execute("UPDATE games SET website=? WHERE id=?", (d["website"], gid))
    esrb = d.get("esrb_rating") or {}
    if isinstance(esrb, dict) and esrb.get("name"):
        conn.execute("UPDATE games SET age_rating=? WHERE id=?", (esrb["name"], gid))
    # cover candidates
    candidates = [listing.get("background_image"), listing.get("background_image_additional")]
    for s in (d.get("short_screenshots") or []):
        u = s.get("image")
        if u: candidates.append(u)
    cover = None
    for u in candidates:
        norm = normalize_rawg_image(u)
        if norm:
            cover = norm; break
    if not cover:
        ss = fetch_screenshots(gid)
        if ss:
            cover = normalize_rawg_image(ss[0].get("image"))
            # store all screenshots
            for s in ss:
                if s.get("id") and s.get("image"):
                    conn.execute("INSERT OR IGNORE INTO screenshots(id, game_id, url) VALUES(?,?,?)", (s["id"], gid, s["image"]))
    if cover:
        conn.execute("UPDATE games SET cover_image=? WHERE id=?", (cover, gid))

    # genres
    for item in (d.get("genres") or []):
        conn.execute("INSERT OR IGNORE INTO genres(id,name) VALUES(?,?)", (item["id"], item["name"]))
        conn.execute("INSERT OR IGNORE INTO game_genres(game_id, genre_id) VALUES(?,?)", (gid, item["id"]))
    # platforms
    for p in (d.get("platforms") or []):
        pi = p.get("platform") or {}
        if pi.get("id") and pi.get("name"):
            conn.execute("INSERT OR IGNORE INTO platforms(id,name) VALUES(?,?)", (pi["id"], pi["name"]))
            conn.execute("INSERT OR IGNORE INTO game_platforms(game_id, platform_id) VALUES(?,?)", (gid, pi["id"]))
    # developers
    for dev in (d.get("developers") or []):
        if dev.get("name"):
            conn.execute("INSERT OR IGNORE INTO game_developers(game_id, developer) VALUES(?,?)", (gid, dev["name"]))
    # publishers
    for pub in (d.get("publishers") or []):
        if pub.get("name"):
            conn.execute("INSERT OR IGNORE INTO game_publishers(game_id, publisher) VALUES(?,?)", (gid, pub["name"]))
    # tags
    for t in (d.get("tags") or []):
        if t.get("name"):
            conn.execute("INSERT OR IGNORE INTO game_tags(game_id, tag) VALUES(?,?)", (gid, t["name"]))
    # series & additions
    for item in fetch_series(gid):
        name, slug = item.get("name"), item.get("slug")
        if name and slug:
            conn.execute("INSERT OR IGNORE INTO game_series_links(game_id, name, url) VALUES(?,?,?)", (gid, name, f"https://rawg.io/games/{slug}"))
    for item in fetch_additions(gid):
        name, slug = item.get("name"), item.get("slug")
        if name and slug:
            conn.execute("INSERT OR IGNORE INTO game_additions_links(game_id, name, url) VALUES(?,?,?)", (gid, name, f"https://rawg.io/games/{slug}"))

def fetch_games():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA foreign_keys=ON")
    # ensure schema
    from db_prepare import main as _prep
    _prep()

    page = 1
    while True:
        data = http_get(f"{BASE}/games", {"key": API_KEY, "page": page, "page_size": 40})
        if not data or not data.get("results"):
            break
        for g in data["results"]:
            upsert_game(conn, g)
            gid = g["id"]
            # try‑harder before skipping
            if not complete_enough(conn, gid):
                enrich(conn, gid, g)
            conn.commit()
        print(f"Committed page {page}")
        page += 1
        # For quota safety, stop after first page by default
        if os.environ.get("LG_FETCH_ALL") != "1":
            break
    conn.close()

if __name__ == "__main__":
    fetch_games()
    print(f"Using DB: {DB}")
    print(f"RAWG key present: {bool(API_KEY and API_KEY != 'YOUR_RAWG_KEY_HERE')}")