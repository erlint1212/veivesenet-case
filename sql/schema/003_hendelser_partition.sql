-- +goose Up
CREATE TABLE IF NOT EXISTS nvdb.hendelser_2023 PARTITION OF nvdb.hendelser
    FOR VALUES FROM (2023) TO (2024);

CREATE TABLE IF NOT EXISTS nvdb.hendelser_2024 PARTITION OF nvdb.hendelser
    FOR VALUES FROM (2024) TO (2025);

CREATE TABLE IF NOT EXISTS nvdb.hendelser_2025 PARTITION OF nvdb.hendelser
    FOR VALUES FROM (2025) TO (2026);

CREATE TABLE IF NOT EXISTS nvdb.hendelser_2026 PARTITION OF nvdb.hendelser
    FOR VALUES FROM (2026) TO (2027);

-- +goose Down
-- Dropping partitions. If the parent table is dropped (in 002_Down),
-- these would be dropped too, but it's cleaner to be explicit.
DROP TABLE IF EXISTS nvdb.hendelser_2023;
DROP TABLE IF EXISTS nvdb.hendelser_2024;
DROP TABLE IF EXISTS nvdb.hendelser_2025;
DROP TABLE IF EXISTS nvdb.hendelser_2026;
