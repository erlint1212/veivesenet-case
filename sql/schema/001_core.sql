-- +goose Up
-- Enable pgcrypto for UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
-- Create the dedicated schema for NVDB data
CREATE SCHEMA IF NOT EXISTS nvdb;

-- +goose Down
-- In a real scenario, you might be hesitant to drop schemas/extensions
-- unless you are sure nothing else depends on them.
-- Dropping in reverse order of creation.
DROP SCHEMA IF EXISTS nvdb;
DROP EXTENSION IF EXISTS "pgcrypto";
