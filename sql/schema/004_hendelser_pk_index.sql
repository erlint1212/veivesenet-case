-- +goose Up
-- Add a composite PRIMARY KEY (id + year) to EACH partition
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2023_pkey') THEN
        ALTER TABLE nvdb.hendelser_2023 ADD PRIMARY KEY (id, "year");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2024_pkey') THEN
        ALTER TABLE nvdb.hendelser_2024 ADD PRIMARY KEY (id, "year");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2025_pkey') THEN
        ALTER TABLE nvdb.hendelser_2025 ADD PRIMARY KEY (id, "year");
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2026_pkey') THEN
        ALTER TABLE nvdb.hendelser_2026 ADD PRIMARY KEY (id, "year");
    END IF;
END;
$$;
-- +goose StatementEnd

-- Add other indexes as needed to partitions (Adding IF NOT EXISTS)
CREATE INDEX IF NOT EXISTS idx_hendelser_2023_veglenkesekvensid ON nvdb.hendelser_2023(veglenkesekvensid);
CREATE INDEX IF NOT EXISTS idx_hendelser_2024_veglenkesekvensid ON nvdb.hendelser_2024(veglenkesekvensid);
CREATE INDEX IF NOT EXISTS idx_hendelser_2025_veglenkesekvensid ON nvdb.hendelser_2025(veglenkesekvensid);
CREATE INDEX IF NOT EXISTS idx_hendelser_2026_veglenkesekvensid ON nvdb.hendelser_2026(veglenkesekvensid);


-- +goose Down
-- Drop indexes first
DROP INDEX IF EXISTS nvdb.idx_hendelser_2023_veglenkesekvensid;
DROP INDEX IF EXISTS nvdb.idx_hendelser_2024_veglenkesekvensid;
DROP INDEX IF EXISTS nvdb.idx_hendelser_2025_veglenkesekvensid;
DROP INDEX IF EXISTS nvdb.idx_hendelser_2026_veglenkesekvensid;

-- Drop primary keys (need to check if they exist first)
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2023_pkey') THEN
        ALTER TABLE nvdb.hendelser_2023 DROP CONSTRAINT hendelser_2023_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2024_pkey') THEN
        ALTER TABLE nvdb.hendelser_2024 DROP CONSTRAINT hendelser_2024_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2025_pkey') THEN
        ALTER TABLE nvdb.hendelser_2025 DROP CONSTRAINT hendelser_2025_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2026_pkey') THEN
        ALTER TABLE nvdb.hendelser_2026 DROP CONSTRAINT hendelser_2026_pkey;
    END IF;
END;
$$;
-- +goose StatementEnd
