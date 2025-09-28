# backend/games_api.py
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException

router = APIRouter()

DB_PATH = "latestgames.db"


# ---------- Utilities ----------

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _split_csv(csv: Optional[str]) -> List[str]:
    if not csv:
        return []
    # GROUP_CONCAT without custom separator uses ","; strip whitespace just in case
    return [s.strip() for s in csv.split(",") if s.strip()]


# ---------- Public Endpoints ----------
@router.get("/games")
def get_games(limit: int = 60, offset: int = 0) -> list[dict]:
    """
    Returns a page of games for the /games index.
    - Never 404s; returns [] when there are no rows.
    - Preserves the card shape your frontend already uses.
    - Safe if some tables (screenshots, genres, platforms) don't exist yet.
    """
    conn = _connect()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Build aggregates defensively
    def table_exists(name: str) -> bool:
        try:
            cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
            return bool(cur.fetchone())
        except sqlite3.OperationalError:
            return False

    has_screens = table_exists("screenshots")
    has_ggenres  = table_exists("game_genres") and table_exists("genres")
    has_gplat    = table_exists("game_platforms") and table_exists("platforms")

    # CTEs guarded by flags
    cte_parts = []
    if has_ggenres:
        cte_parts.append("""
            genres_agg AS (
              SELECT gg.game_id, GROUP_CONCAT(DISTINCT g.name) AS genres_csv
              FROM game_genres gg
              JOIN genres g ON g.id = gg.genre_id
              GROUP BY gg.game_id
            )
        """)
    if has_gplat:
        cte_parts.append("""
            platforms_agg AS (
              SELECT gp.game_id, GROUP_CONCAT(DISTINCT p.name) AS platforms_csv
              FROM game_platforms gp
              JOIN platforms p ON p.id = gp.platform_id
              GROUP BY gp.game_id
            )
        """)
    if has_screens:
        cte_parts.append("""
            first_shot AS (
              SELECT s.game_id, MIN(s.id) AS first_id
              FROM screenshots s
              GROUP BY s.game_id
            )
        """)

    with_clause = ("WITH " + ",\n".join(cte_parts)) if cte_parts else ""

    # SELECT with LEFT JOINs that are present; skip absent ones
    select_sql = f"""
        {with_clause}
        SELECT
          games.id,
          games.slug,
          games.name,
          games.released,
          games.rating,
          games.metascore_number,
          games.metascore_color,
          /* cover/thumbnail fallback logic */
          COALESCE(
             {"ss.url," if has_screens else ""}
             games.cover_image
          ) AS screenshot,
          games.cover_image
          {" , ga.genres_csv"     if has_ggenres else ""}
          {" , pa.platforms_csv"  if has_gplat   else ""}
        FROM games
        {" LEFT JOIN genres_agg ga ON ga.game_id = games.id" if has_ggenres else ""}
        {" LEFT JOIN platforms_agg pa ON pa.game_id = games.id" if has_gplat else ""}
        {" LEFT JOIN first_shot fs ON fs.game_id = games.id" if has_screens else ""}
        {" LEFT JOIN screenshots ss ON ss.id = fs.first_id" if has_screens else ""}
        ORDER BY COALESCE(games.metascore_number, 0) DESC, games.rating DESC, games.name COLLATE NOCASE ASC
        LIMIT ? OFFSET ?;
    """

    rows = []
    try:
        cur.execute(select_sql, (limit, offset))
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        # If the 'games' table itself is missing, return []
        conn.close()
        return []

    out: list[dict] = []
    for r in rows:
        # r keys present regardless of CTEs
        item = {
            "id": r["id"],
            "slug": r["slug"],
            "name": r["name"],
            "released": r["released"],
            "rating": r["rating"],
            "metascore_number": r["metascore_number"],
            "metascore_color": r["metascore_color"],
            "screenshot": r["screenshot"],     # thumbnail for the card
            "cover_image": r["cover_image"],   # keep existing field for detail fallback
        }
        # Add arrays if we had aggregates; otherwise default to []
        item["genres"]    = _split_csv(r["genres_csv"])    if ("genres_csv" in r.keys()) else []
        item["platforms"] = _split_csv(r["platforms_csv"]) if ("platforms_csv" in r.keys()) else []

        out.append(item)

    conn.close()
    return out
    
@router.get("/games/{slug}")
def get_game(slug: str) -> dict:
    """
    Detail for a single game by slug.
    Keeps existing fields and adds: developers, publishers, tags, website, age_rating,
    description, screenshots (list), media (list of images), stores (objects),
    and suggestions (objects).
    """
    conn = _connect()
    cur = conn.cursor()

    # --- Base row (exactly as before, plus safe optional columns via try/except) ---
    cur.execute(
        """
        WITH genres_agg AS (
            SELECT gg.game_id,
                   GROUP_CONCAT(DISTINCT g.name) AS genres_csv
            FROM game_genres gg
            JOIN genres g ON g.id = gg.genre_id
            GROUP BY gg.game_id
        ),
        platforms_agg AS (
            SELECT gp.game_id,
                   GROUP_CONCAT(DISTINCT p.name) AS platforms_csv
            FROM game_platforms gp
            JOIN platforms p ON p.id = gp.platform_id
            GROUP BY gp.game_id
        ),
        first_shot AS (
            SELECT s.game_id, MIN(s.id) AS first_id
            FROM screenshots s
            GROUP BY s.game_id
        )
        SELECT
            games.id,
            games.slug,
            games.name,
            games.released,
            games.rating,
            games.metascore_number,
            games.metascore_color,
            games.cover_image,
            ga.genres_csv,
            pa.platforms_csv,
            s.url AS screenshot
        FROM games
        LEFT JOIN genres_agg    ga ON ga.game_id = games.id
        LEFT JOIN platforms_agg pa ON pa.game_id = games.id
        LEFT JOIN first_shot    fs ON fs.game_id = games.id
        LEFT JOIN screenshots    s ON s.id = fs.first_id
        WHERE games.slug = ?
        LIMIT 1;
        """,
        (slug,),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Game not found")

    game_id = row["id"]

    # Helper: safely query a single column from games (if column doesn't exist, return None)
    def safe_scalar(column: str):
        try:
            c2 = conn.execute(f"SELECT {column} FROM games WHERE id = ? LIMIT 1;", (game_id,))
            r2 = c2.fetchone()
            return r2[0] if r2 and r2[0] is not None else None
        except sqlite3.OperationalError:
            return None

    # Helper: safely return list[str] via join tables; if tables/columns missing, return []
    def safe_group(sql: str, params: tuple) -> list[str]:
        try:
            c2 = conn.execute(sql, params)
            return [r[0] for r in c2.fetchall() if r and r[0]]
        except sqlite3.OperationalError:
            return []

    # ---------- Optional scalar fields on games ----------
    website     = safe_scalar("website")
    age_rating  = safe_scalar("age_rating")
    description = safe_scalar("description")

    # ---------- Developers / Publishers / Tags ----------
    developers = safe_group(
        """
        SELECT DISTINCT d.name
        FROM game_developers gd
        JOIN developers d ON d.id = gd.developer_id
        WHERE gd.game_id = ?
        """,
        (game_id,),
    )
    publishers = safe_group(
        """
        SELECT DISTINCT pu.name
        FROM game_publishers gp
        JOIN publishers pu ON pu.id = gp.publisher_id
        WHERE gp.game_id = ?
        """,
        (game_id,),
    )
    tags = safe_group(
        """
        SELECT DISTINCT t.name
        FROM game_tags gt
        JOIN tags t ON t.id = gt.tag_id
        WHERE gt.game_id = ?
        """,
        (game_id,),
    )

    # ---------- Screenshots (full list) ----------
    screenshots: list[str] = []
    try:
        c2 = conn.execute(
            """
            SELECT COALESCE(local_path, url) AS src
            FROM screenshots
            WHERE game_id = ?
            ORDER BY COALESCE(sort_order, 999999), id
            """,
            (game_id,),
        )
        screenshots = [r[0] for r in c2.fetchall() if r and r[0]]
    except sqlite3.OperationalError:
        screenshots = []

    # Map screenshots to MediaGallery’s expected shape (images only for now)
    media = [{"type": "image", "url": src, "preview_url": None, "position": i} for i, src in enumerate(screenshots)]

    # ---------- Stores (detailed objects for StoreButtons) ----------
    stores: list[dict] = []
    try:
        c2 = conn.execute(
            """
            SELECT s.id AS store_id, s.name, s.slug, s.domain, gs.url,
                   s.logo_url, s.hover_image_url
            FROM game_stores gs
            JOIN stores s ON s.id = gs.store_id
            WHERE gs.game_id = ?
            ORDER BY s.name COLLATE NOCASE ASC, s.id ASC
            """,
            (game_id,),
        )
        for r in c2.fetchall():
            stores.append(
                {
                    "store_id": r["store_id"],
                    "name": r["name"],
                    "slug": r["slug"],
                    "domain": r["domain"],
                    "url": r["url"],
                    "logo_url": r["logo_url"],
                    "hover_image_url": r["hover_image_url"],
                }
            )
    except sqlite3.OperationalError:
        stores = []

    # ---------- Suggestions (more like this) ----------
    suggestions: list[dict] = []
    try:
        # Aggregate genres/platforms for suggested games too
        c2 = conn.execute(
            """
            WITH ga AS (
              SELECT gg.game_id, GROUP_CONCAT(DISTINCT g.name) AS genres_csv
              FROM game_genres gg
              JOIN genres g ON g.id = gg.genre_id
              GROUP BY gg.game_id
            ),
            pa AS (
              SELECT gp.game_id, GROUP_CONCAT(DISTINCT p.name) AS platforms_csv
              FROM game_platforms gp
              JOIN platforms p ON p.id = gp.platform_id
              GROUP BY gp.game_id
            ),
            first_shot AS (
              SELECT s.game_id, MIN(s.id) AS first_id
              FROM screenshots s
              GROUP BY s.game_id
            )
            SELECT
              sug.position,
              s.id          AS suggested_id,
              s.name,
              COALESCE(ss.url, s.cover_image) AS image_url,
              pa.platforms_csv,
              s.metascore_number,
              s.metascore_color,
              s.released,
              ga.genres_csv
            FROM suggestions sug
            JOIN games s          ON s.id = sug.suggested_game_id
            LEFT JOIN ga          ON ga.game_id = s.id
            LEFT JOIN pa          ON pa.game_id = s.id
            LEFT JOIN first_shot  fs ON fs.game_id = s.id
            LEFT JOIN screenshots ss ON ss.id = fs.first_id
            WHERE sug.game_id = ?
            ORDER BY sug.position ASC, s.name COLLATE NOCASE ASC
            LIMIT 24
            """,
            (game_id,),
        )
        for r in c2.fetchall():
            suggestions.append(
                {
                    "position": r["position"],
                    "suggested_id": r["suggested_id"],
                    "name": r["name"],
                    "image_url": r["image_url"],
                    "platforms_csv": r["platforms_csv"],
                    "metascore_number": r["metascore_number"],
                    "metascore_color": r["metascore_color"],
                    "released": r["released"],
                    "genres_csv": r["genres_csv"],
                }
            )
    except sqlite3.OperationalError:
        suggestions = []

    # ---------- Assemble response (includes everything you already returned) ----------
    out = {
        "id": row["id"],
        "slug": row["slug"],
        "name": row["name"],
        "cover_image": row["cover_image"],
        "screenshot": row["screenshot"],  # first-shot convenience
        "released": row["released"],
        "rating": row["rating"],
        "metascore_number": row["metascore_number"],
        "metascore_color": row["metascore_color"],
        "genres": _split_csv(row["genres_csv"]),
        "platforms": _split_csv(row["platforms_csv"]),
        # New fields
        "website": website,
        "age_rating": age_rating,
        "description": description,
        "developers": developers,
        "publishers": publishers,
        "tags": tags,
        "screenshots": screenshots,
        "media": media,
        "stores": stores,
        "suggestions": suggestions,
    }

    conn.close()
    return out
