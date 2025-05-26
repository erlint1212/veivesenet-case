-- +goose Up
CREATE TABLE IF NOT EXISTS nvdb.hendelser (
    id UUID DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    veglenkesekvensid BIGINT,
    relativ_posisjon DOUBLE PRECISION,
    vegvedlikehold TEXT,
    rand_float DOUBLE PRECISION,
    "year" INTEGER NOT NULL
) PARTITION BY RANGE ("year");

-- +goose Down
DROP TABLE IF EXISTS nvdb.hendelser;
