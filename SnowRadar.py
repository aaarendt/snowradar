# SnowRadar.py

# code to ingest snow radar data to ice2oceans database

# aaarendt 20160104

import pandas as pd
from sqlalchemy import create_engine
import sys
sys.path.append('c:\work\src')
import settings as s # contains password info. Not included in git repo for security.
import glob

# get connection string from settings.py
cs = getattr(s,'AWS_localhost')

# connect to the database
engine = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password'])[2:-1] + '@' + cs['host'] + ':' + cs['port'] + '/' + cs['dbname'])

rootFolder = r'c:/work/mnt/sweData/'

fileList = glob.glob(rootFolder + '*.txt')

df = pd.DataFrame()
for file in fileList:
    data = pd.read_csv(file, sep = '\t')
    df = df.append(data, ignore_index = True)

# create a temporary database table to later SET with the master table. Make sure the temporary table name does not yet exist       

df = pd.read_excel(rootFolder + 'scott_glacier_2013_helo_swe.xlsx')

dbnamePts = 'sweingest'
df.to_sql(dbnamePts,engine, index = False)
# create the geometry field
engine.execute("""ALTER TABLE %s ADD COLUMN geom geometry(Point, 3338);""" %(dbnamePts)) 
# populate the geometry field
engine.execute("""UPDATE %s SET geom = ST_Transform(ST_setSRID(ST_MakePoint(long,lat),4326),3338);""" %(dbnamePts))
# generate the primary key
engine.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbnamePts))
engine.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbnamePts, dbnamePts))
engine.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbnamePts))

# make the associated line file

dbnameLines = 'sweingest_lines'

engine.execute("""CREATE TABLE %s (collection text, geom geometry(Linestring, 3338));""" %(dbnameLines))
query = 'WITH linecreation AS (SELECT collection, ST_MakeLine(geom) as geom FROM ' + dbnamePts + ' GROUP BY collection) INSERT INTO ' + dbnameLines + ' SELECT * FROM linecreation;'
# not sure why this doesn't work in line
engine.execute(query)
# generate the primary key
engine.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbnameLines))
engine.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbnameLines, dbnameLines))
engine.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbnameLines))


# ingest the metadata to a pandas dataframe
fileList = glob.glob(rootFolder + 'metalines/*.txt')

df = pd.DataFrame()
for file in fileList:
    data = pd.read_csv(file, sep = '\t')
    df = df.append(data, ignore_index = True)

df['date'] = pd.to_datetime(df['date'], format = "%m/%d/%Y")

dbnameMeta = 'sweingest_metadata'
df.to_sql(dbnameMeta,engine, index = False)
# generate the primary key
engine.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbnameMeta))
engine.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbnameMeta, dbnameMeta))
engine.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbnameMeta))

# merge the geometries into the metadata table

# first make a new geom column
engine.execute("""ALTER TABLE %s ADD COLUMN geom geometry(Linestring, 3338);""" % (dbnameMeta))
engine.execute("""UPDATE %s AS sm SET geom = l.geom FROM (SELECT geom, collection FROM %s) AS l WHERE sm.collection = l.collection;""" % (dbnameMeta, dbnameMeta))

# manually move these to the master database and merge

# a bit of the manual work got ugly:

# the 2013 lines were multiLineStrings and I had to convert:
# should be a one time thing? check this was ok?

#ALTER TABLE sl ADD COLUMN geom2 geometry(Linestring, 3338);
#UPDATE sl SET geom2 = ST_LineMerge(s.geom)
#FROM 
#(SELECT geom,collection FROM snowradar_lines) AS s
#WHERE sl.collection = s.collection; 

# create temporary merged tables before committing:

query = """CREATE TABLE snowradar AS \
(SELECT elev, twtt, thickness, swe, trace, geom FROM sweingest \
UNION \
SELECT elev, twtt, thickness, swe, trace, geom FROM snowradar_old);"""
engine.execute(query)

dbname = 'snowradar'
# generate the primary key
engine.execute("""ALTER TABLE %s ADD COLUMN gid SERIAL;""" %(dbname))
engine.execute("""UPDATE %s SET gid = nextval(pg_get_serial_sequence('%s','gid'));""" %(dbname, dbname))
engine.execute("""ALTER TABLE %s ADD PRIMARY KEY(gid);""" %(dbname))
