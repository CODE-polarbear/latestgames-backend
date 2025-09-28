import sqlite3, csv, argparse
from io import StringIO

def export(db, out, root=None):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT id, slug, name,
               IFNULL(released,'') AS released,
               IFNULL(rating,'')   AS rating,
               IFNULL(cover_image,'') AS cover_image,
               IFNULL(website,'')     AS website,
               IFNULL(age_rating,'')  AS age_rating,
               IFNULL(description,'') AS about
        FROM games ORDER BY IFNULL(released,'0000-00-00') DESC, id DESC
    """)
    games = cur.fetchall()
    def map_list(sql):
        m = {}
        for row in conn.execute(sql):
            m.setdefault(row["game_id"], []).append(dict(row))
        return m
    genres_map = map_list("""SELECT gg.game_id, ge.name AS name FROM game_genres gg JOIN genres ge ON ge.id=gg.genre_id""")
    plats_map  = map_list("""SELECT gp.game_id, pf.name AS name FROM game_platforms gp JOIN platforms pf ON pf.id=gp.platform_id""")
    devs_map   = map_list("""SELECT game_id, developer AS name FROM game_developers""")
    pubs_map   = map_list("""SELECT game_id, publisher AS name FROM game_publishers""")
    tags_map   = map_list("""SELECT game_id, tag AS name FROM game_tags""")
    series_map = map_list("""SELECT game_id, name, url FROM game_series_links""")
    adds_map   = map_list("""SELECT game_id, name, url FROM game_additions_links""")
    shots_map  = map_list("""SELECT game_id, url FROM screenshots""")

    def semijoin(rows, key):
        return "; ".join(sorted({(r.get(key) or "").strip() for r in rows if r.get(key)}))

    fieldnames = [
        "id","slug","name","released","rating",
        "genres","platforms",
        "developers","developers_list",
        "publishers","publishers_list",
        "tags",
        "age_rating","website","cover_image","about",
        "series_names","series_urls","additions_names","additions_urls",
        "num_screenshots","first_screenshot"
    ]

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for g in games:
            gid = g["id"]
            genres = semijoin(genres_map.get(gid, []), "name")
            plats  = semijoin(plats_map.get(gid, []),  "name")
            devs   = semijoin(devs_map.get(gid, []),   "name")
            pubs   = semijoin(pubs_map.get(gid, []),   "name")
            tags   = semijoin(tags_map.get(gid, []),   "name")
            series = series_map.get(gid, []) or []
            adds   = adds_map.get(gid, []) or []
            series_names = "; ".join([s["name"] for s in series if s.get("name")])
            series_urls  = "; ".join([s["url"] for s in series if s.get("url")])
            adds_names   = "; ".join([s["name"] for s in adds if s.get("name")])
            adds_urls    = "; ".join([s["url"] for s in adds if s.get("url")])
            shots = shots_map.get(gid, []) or []
            num_shots = len(shots)
            first_shot = shots[0]["url"] if shots else ""
            w.writerow({
                "id": gid, "slug": g["slug"], "name": g["name"],
                "released": g["released"], "rating": g["rating"],
                "genres": genres, "platforms": plats,
                "developers": devs, "developers_list": devs,
                "publishers": pubs, "publishers_list": pubs,
                "tags": tags, "age_rating": g["age_rating"], "website": g["website"],
                "cover_image": g["cover_image"], "about": g["about"],
                "series_names": series_names, "series_urls": series_urls,
                "additions_names": adds_names, "additions_urls": adds_urls,
                "num_screenshots": num_shots, "first_screenshot": first_shot,
            })
    conn.close()
    print(f"✅ Wrote CSV: {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="latestgames.db")
    ap.add_argument("--out", default="manifest.csv")
    ap.add_argument("--root", default=None, help="(unused placeholder for now)")
    args = ap.parse_args()
    export(args.db, args.out, args.root)
