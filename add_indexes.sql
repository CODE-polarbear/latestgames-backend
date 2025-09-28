CREATE INDEX IF NOT EXISTS idx_game_genres_gid ON game_genres(game_id);
CREATE INDEX IF NOT EXISTS idx_game_platforms_gid ON game_platforms(game_id);
CREATE INDEX IF NOT EXISTS idx_game_devs_gid ON game_developers(game_id);
CREATE INDEX IF NOT EXISTS idx_game_pubs_gid ON game_publishers(game_id);
CREATE INDEX IF NOT EXISTS idx_game_tags_gid ON game_tags(game_id);
CREATE INDEX IF NOT EXISTS idx_series_gid ON game_series_links(game_id);
CREATE INDEX IF NOT EXISTS idx_additions_gid ON game_additions_links(game_id);
CREATE INDEX IF NOT EXISTS idx_shots_gid ON screenshots(game_id);
