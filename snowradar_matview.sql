-- MATERIALIZED VIEW to link the collection name back to the snow radar points
-- because doing a spatial join each time was way too slow

CREATE MATERIALIZED VIEW snowradar_q AS
SELECT s.elev, s.swe, l.date, l.collection, l.geom FROM snowradar AS s, snowradar_lines AS l
WHERE ST_Intersects(l.geom, s.geom);

CREATE UNIQUE INDEX snowradar_collection
  ON snowradar_query (collection);