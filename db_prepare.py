import os, sqlite3
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
DB_FILE = os.environ.get("LG_DB", "latestgames.db")

def main():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS games(
        id INTEGER PRIMARY KEY,
        slug TEXT,
        name TEXT,
        description TEXT,
        released TEXT,
        rating REAL
    );""")
    def add_col(col, typ):
        try:
            c.execute(f"ALTER TABLE games ADD COLUMN {col} {typ};")
            conn.commit()
        except Exception:
            pass
    for col, typ in [
        ("about","TEXT"),("cover_image","TEXT"),("cover_thumb","TEXT"),
        ("website","TEXT"),("age_rating","TEXT"),("age","INTEGER"),
    ]:
        add_col(col, typ)
    c.executescript("""
    CREATE TABLE IF NOT EXISTS genres(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE IF NOT EXISTS platforms(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE IF NOT EXISTS developers(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE IF NOT EXISTS publishers(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE IF NOT EXISTS tags(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE IF NOT EXISTS game_genres(game_id INTEGER, genre_id INTEGER, PRIMARY KEY(game_id, genre_id));
    CREATE TABLE IF NOT EXISTS game_platforms(game_id INTEGER, platform_id INTEGER, PRIMARY KEY(game_id, platform_id));
    CREATE TABLE IF NOT EXISTS game_developers(game_id INTEGER, developer_id INTEGER, PRIMARY KEY(game_id, developer_id));
    CREATE TABLE IF NOT EXISTS game_publishers(game_id INTEGER, publisher_id INTEGER, PRIMARY KEY(game_id, publisher_id));
    CREATE TABLE IF NOT EXISTS game_tags(game_id INTEGER, tag_id INTEGER, PRIMARY KEY(game_id, tag_id));
    CREATE TABLE IF NOT EXISTS screenshots(id INTEGER PRIMARY KEY, game_id INTEGER, url TEXT);
    CREATE TABLE IF NOT EXISTS series_entries(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, name TEXT, url TEXT);
    CREATE TABLE IF NOT EXISTS dlc_entries(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, name TEXT, url TEXT);
    CREATE TABLE IF NOT EXISTS game_series_links(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, name TEXT, url TEXT);
    CREATE TABLE IF NOT EXISTS game_additions_links(id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, name TEXT, url TEXT);
    """)
    for stmt in [
        "CREATE INDEX IF NOT EXISTS idx_game_publishers_gid ON game_publishers(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_game_tags_gid ON game_tags(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_series_gid ON game_series_links(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_additions_gid ON game_additions_links(game_id)"
    ]:
        try:
            c.execute(stmt)
        except Exception:
            pass
    conn.commit()
    conn.close()
    print(f"✅ DB prepared: {DB_FILE}")

if __name__ == "__main__":
    main()