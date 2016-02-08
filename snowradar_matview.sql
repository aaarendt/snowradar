-- MATERIALIZED VIEW to link the collection name back to the snow radar points
-- because doing a spatial join each time was way too slow

CREATE MATERIALIZED VIEW snowradar_query AS
SELECT s.elev, s.swe, l.collection FROM snowradar AS s, snowradar_lines AS l
WHERE ST_Intersects(l.geom, s.geom);