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

@router.get("/games/{slug}")
def get_game(slug: str) -> dict:
    """
    Detail for a single game by slug.
    Returns the same field names as /games list + released, metascore fields.
    """
    conn = _connect()
    cur = conn.cursor()

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
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Game not found")

    return {
        "id": row["id"],
        "slug": row["slug"],
        "name": row["name"],
        "cover_image": row["cover_image"],
        "screenshot": row["screenshot"],
        "released": row["released"],
        "rating": row["rating"],
        "metascore_number": row["metascore_number"],
        "metascore_color": row["metascore_color"],
        "genres": _split_csv(row["genres_csv"]),
        "platforms": _split_csv(row["platforms_csv"]),
    }

@router.get("/games")
def list_games(limit: int = 60, offset: int = 0, order: str = "top") -> List[Dict[str, Any]]:
    """
    Lightweight list for the /games index grid.
    Fields align with your Next.js expectations.
    order: "top" | "newest" | "latest" | "alpha"
    """
    order_sql = {
        "top": "games.rating DESC, games.id ASC",
        "newest": "date(games.released) DESC, games.id DESC",
        "latest": "games.id DESC",
        "alpha": "lower(games.name) ASC, games.id ASC",
    }.get(order, "games.rating DESC, games.id ASC")

    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        f"""
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
        LEFT JOIN genres_agg   ga ON ga.game_id = games.id
        LEFT JOIN platforms_agg pa ON pa.game_id = games.id
        LEFT JOIN first_shot   fs ON fs.game_id = games.id
        LEFT JOIN screenshots   s ON s.id = fs.first_id
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?;
        """,
        (limit, offset),
    )
    rows = cur.fetchall()
    conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "slug": r["slug"],
                "name": r["name"],
                "cover_image": r["cover_image"],
                "screenshot": r["screenshot"],
                "released": r["released"],
                "rating": r["rating"],
                "metascore_number": r["metascore_number"],
                "metascore_color": r["metascore_color"],
                "genres": _split_csv(r["genres_csv"]),
                "platforms": _split_csv(r["platforms_csv"]),
            }
        )
    return out
