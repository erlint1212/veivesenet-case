-- +goose Up
CREATE TABLE IF NOT EXISTS nvdb.vegobjekter_fartsgrense (
    nvdb_id BIGINT NOT NULL,           -- Primary Key: The unique ID from NVDB
    vegkategori TEXT,                  -- e.g., 'E', 'F', 'K'
    fylke INTEGER,                     -- County number
    kommune INTEGER,                   -- Municipality number
    veglenkesekvensid BIGINT,          -- **CRUCIAL for joining with hendelser**
    startdato TIMESTAMP WITH TIME ZONE, -- Start date of the object's validity
    sist_modifisert TIMESTAMP WITH TIME ZONE, -- Last modification date
    geometri_wkt TEXT,                 -- Geometry as Well-Known Text
    fartsgrense INTEGER,               -- The actual speed limit value
    CONSTRAINT vegobjekter_fartsgrense_pkey PRIMARY KEY (nvdb_id)
);

-- +goose Down
DROP TABLE IF EXISTS nvdb.vegobjekter_fartsgrense;
