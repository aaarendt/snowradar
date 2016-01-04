# SnowRadar.py

# code to ingest snow radar data to ice2oceans database

# aaarendt 20160104

import pandas as pd
from sqlalchemy import create_engine
import settings as s # contains password info. Not included in git repo for security.

# get connection string from settings.py
cs = getattr(s,'localhost_SurfaceBook')

# connect to the database
engine = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password'])[2:-1] + '@' + cs['host'] + ':' + cs['port'] + '/' + cs['dbname'])

# ingest the data to a pandas dataframe
data = pd.read_csv(r'c:/work/mnt/sweData/gulkana_2014.txt',sep = '\t')

# I don't think we need this if the column names are consistent from the start
#data.columns=['trace','long','lat','elev','twtt','thickness','swe','collection']

# for when we start looping

#f irst = 1
#if first:
#    df = data
#    first = 0
#else:
#    df = df.append(data)

# this performs the ingest to the database. Make sure the table name does not yet exist       

dbnamePts = "gulkpoints"
data.to_sql(%s, engine) %(dbname)

# create the geometry field
engine.execute("""ALTER TABLE %s ADD COLUMN geom geometry(Point, 3338);""" %(dbnamePts)) 
# populate the geometry field
engine.execute("""UPDATE swe_ingest8 SET geom = ST_TRANSFORM(ST_setSRID(ST_MakePoint(long,lat),4326),3338);""" %(dbnamePts))
# generate the primary key
engine.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbnamePts))
engine.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbname, dbnamePts))
engine.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbnamePts))

# make the associated line file


# ingest the metadata to a pandas dataframe
data = pd.read_csv(r'c:/work/mnt/sweData/gulkana_2014_meta_lines.txt',sep = '\t')

dbnameLines = 'gulkLines"
engine.execute("""CREATE TABLE %s (collection text, geom geometry(Linestring, 3338));""" %(dbnameLines))
engine.execute("""WITH upd AS 
(SELECT collection, ST_MakeLine(geom) as newgeom FROM %s GROUP BY collection)
INSERT INTO %s SELECT * FROM upd;""" %(dbnamePts, dbnameLines))

# something in here to merge the metatadata attributes with the geometries

# generate the primary key
engine.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbnameLines))
engine.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbname, dbnameLines))
engine.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbnameLines))

print ('done')