-- +goose Up
-- Create the partition for the year 2022
CREATE TABLE IF NOT EXISTS nvdb.hendelser_2022 PARTITION OF nvdb.hendelser
    FOR VALUES FROM (2022) TO (2023);

-- Add the primary key for the new partition
-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'hendelser_2022_pkey') THEN
        ALTER TABLE nvdb.hendelser_2022 ADD PRIMARY KEY (id, "year");
    END IF;
END;
$$;
-- +goose StatementEnd

-- Add the index for the new partition
CREATE INDEX IF NOT EXISTS idx_hendelser_2022_veglenkesekvensid ON nvdb.hendelser_2022(veglenkesekvensid);


-- +goose Down
-- Drop the 2022 partition if rolling back
DROP TABLE IF EXISTS nvdb.hendelser_2022;
