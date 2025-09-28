import sqlite3, os, sys

DB = sys.argv[1] if len(sys.argv) > 1 else "latestgames.db"

def add_col(cur, table, col, typ):
    # emulate "add if missing"
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")

def main():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    # base games
    c.execute("""CREATE TABLE IF NOT EXISTS games(
        id INTEGER PRIMARY KEY,
        slug TEXT, name TEXT, description TEXT,
        released TEXT, rating REAL, cover_image TEXT,
        website TEXT, age_rating TEXT
    )""")
    # lookup
    c.execute("CREATE TABLE IF NOT EXISTS genres(id INTEGER PRIMARY KEY, name TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS platforms(id INTEGER PRIMARY KEY, name TEXT)")
    # junctions
    c.execute("CREATE TABLE IF NOT EXISTS game_genres(game_id INTEGER, genre_id INTEGER, PRIMARY KEY(game_id, genre_id))")
    c.execute("CREATE TABLE IF NOT EXISTS game_platforms(game_id INTEGER, platform_id INTEGER, PRIMARY KEY(game_id, platform_id))")
    # new simple tables
    c.execute("CREATE TABLE IF NOT EXISTS game_developers(game_id INTEGER, developer TEXT, PRIMARY KEY(game_id, developer))")
    c.execute("CREATE TABLE IF NOT EXISTS game_publishers(game_id INTEGER, publisher TEXT, PRIMARY KEY(game_id, publisher))")
    c.execute("CREATE TABLE IF NOT EXISTS game_tags(game_id INTEGER, tag TEXT, PRIMARY KEY(game_id, tag))")
    c.execute("CREATE TABLE IF NOT EXISTS game_series_links(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, name TEXT, url TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS game_additions_links(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, name TEXT, url TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS screenshots(id INTEGER PRIMARY KEY, game_id INTEGER, url TEXT)")

    # ensure columns on games
    for col, typ in [("cover_image","TEXT"), ("website","TEXT"), ("age_rating","TEXT"), ("description","TEXT"), ("released","TEXT"), ("rating","REAL")]:
        add_col(c, "games", col, typ)

    # indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_game_genres_gid ON game_genres(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_game_platforms_gid ON game_platforms(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_game_devs_gid ON game_developers(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_game_pubs_gid ON game_publishers(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_game_tags_gid ON game_tags(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_series_gid ON game_series_links(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_additions_gid ON game_additions_links(game_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shots_gid ON screenshots(game_id)")

    conn.commit()
    conn.close()
    print(f"✅ DB prepared: {DB}")

if __name__ == "__main__":
    main()
