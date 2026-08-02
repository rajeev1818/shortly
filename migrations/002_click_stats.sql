CREATE TABLE IF NOT EXISTS click_stats (
    short_code  TEXT NOT NULL,
    bucket_hour TIMESTAMPTZ NOT NULL,
    clicks      BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (short_code, bucket_hour)
);
