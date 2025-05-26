-- +goose Up
CREATE INDEX IF NOT EXISTS idx_vegobj_fart_fylke ON nvdb.vegobjekter_fartsgrense(fylke);
CREATE INDEX IF NOT EXISTS idx_vegobj_fart_kommune ON nvdb.vegobjekter_fartsgrense(kommune);
CREATE INDEX IF NOT EXISTS idx_vegobj_fart_veglenke ON nvdb.vegobjekter_fartsgrense(veglenkesekvensid);

-- +goose Down
DROP INDEX IF EXISTS nvdb.idx_vegobj_fart_fylke;
DROP INDEX IF EXISTS nvdb.idx_vegobj_fart_kommune;
DROP INDEX IF EXISTS nvdb.idx_vegobj_fart_veglenke;
