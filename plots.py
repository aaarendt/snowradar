import pandas as pd
from sqlalchemy import create_engine
import glob
import sys
sys.path.append(r'C:\work\src\ingestAltimetry')
import settings as s
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')
import seaborn

# get connection string from settings.py
cs = getattr(s,'AWS_localhost')

# connect to the database
engine = create_engine('postgresql://' + cs['user'] + ':' + str(cs['password'])[2:-1] + '@' + cs['host'] + ':' + cs['port'] + '/' + cs['dbname'])

glacier = 'Gulkana'
query = "SELECT sr.elev, sr.swe, sr.collection, srl.date FROM (SELECT swe, elev, collection FROM \
        snowradar_query WHERE collection LIKE '"+ glacier + "%%') AS sr JOIN (SELECT date, collection \
        FROM snowradar_lines WHERE collection LIKE '" + glacier + "%%') AS srl ON sr.collection = srl.collection"

query = "SELECT sr.elev, sr.swe, split_part(sr.collection,'_', 1) AS glacier, srl.date FROM (SELECT swe, elev, collection FROM \
        snowradar_query) AS sr JOIN (SELECT date, collection \
        FROM snowradar_lines) AS srl ON sr.collection = srl.collection"

# note that any instance of % from postgresql requires %% in Python

query = "SELECT sr.elev AS elev, AVG(sr.swe) AS swe, split_part(sr.collection,'_', 1) AS glacier \
FROM (SELECT swe, (elev - elev::int %% 10 + 5) AS elev, collection FROM snowradar_query) \
AS sr JOIN (SELECT date, collection FROM snowradar_lines) \
AS srl ON sr.collection = srl.collection GROUP BY glacier, elev"

df = pd.read_sql(query, engine)
t = pd.pivot_table(df,index='elev', columns='glacier', values='swe')

glacierList = ''
for glacier in df.glacier.unique():
   glacierList += "'" + glacier + " Glacier',"
glacierList = glacierList[:-1]

query = "SELECT elevation AS elev, balance, split_part(name, ' ', 1) AS glacier FROM point_balances WHERE name IN (" + glacierList + ")  \
         AND season = 'w'"

pb = pd.read_sql(query ,engine)
pb.elev = (pb.elev - pb.elev % 10 + 5) # Kilroy
pointBal = pd.pivot_table(pb, index = 'elev', columns = 'glacier', values = 'balance')
pointBal.columns = ['Eklutna_stake','Gulkana_stake','Wolverine_stake']

colors = {2013:'gray',2014:'blue',2015:'orange'}
colors = {'Gulkana':'gray', 'Wolverine':'blue','Eklutna':'orange','Scott':'green','Taku':'red','Valdez':'purple','Eureka':'brown'}



all = pd.concat([t, pointBal], axis = 1, join_axes = [t.index])
all['elevation'] = all.index

# try
# swe = df.groupby(['elev','collection']).swe.mean()

first = True
i=0
#for key, grp in df.groupby(df['date'].map(lambda x: x.year)):
#for key, grp in df.groupby(df.glacier):

for key, grp in df.groupby(['elev','collection']):
    print(key)
    if first:
       #ax = grp.plot(subplots = True, x='elev', y = 'swe', kind = 'scatter', s = 1, c = colors[key], edgecolors = 'none', label = key)
       ax = grp.plot(subplots = True, x='elev', y = 'swe', kind = 'box',  label = key)
       first = False
    else:   
       #grp.plot(subplots = True, x='elev', y = 'swe', kind = k, ax = ax, s = 1, c = colors[key], edgecolors = 'none', label = key)
       grp.plot(subplots = True, x='elev', y = 'swe', kind = 'box', ax = ax,  label = key)
    i += 1
savefig(r"C:\Users\Anthony Arendt\Desktop\test.png", dpi = 600)

first = True
for gl in ('Gulkana','Gulkana_stake'): #('Wolverine', 'Wolverine_stake')
   if first:
      ax = all.plot(subplots = True, x = 'elevation', y = gl, kind = 'scatter', c = 'gray', style = '-', label = 'radar')
      first = False
   else:
      all.plot(subplots = True, x = 'elevation', y = gl, kind = 'scatter', label = 'conventional', c = 'red', ax = ax)

plt.ylabel(r'Mass balance (kg m$^{-2}$)')
plt.title('Gulkana Glacier')
plt.legend(loc='lower right')
savefig(r"C:\Users\Anthony Arendt\Desktop\Gulkana.png", dpi = 600)

