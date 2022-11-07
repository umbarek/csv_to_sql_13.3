import csv
import sqlite3
from sqlite3 import Error
from sqlalchemy import create_engine

#CREATE CONNECTION
conn = sqlite3.connect('database.db')

#CREATE CURSOR
cursor = conn.cursor()

#CREATE TABLES
create_measure = """
   CREATE TABLE IF NOT EXISTS measure (
      station VARCHAR(11),
      date VARCHAR(10) NOT NULL,
      precip float NOT NULL,
      tobs int NOT NULL
   );
   """   

create_stations = """
   CREATE TABLE IF NOT EXISTS stations (
      station VARCHAR(11),
      latitude float NOT NULL,
      lognitude float NOT NULL,
      elevation float NOT NULL,
      name text NOT NULL,
      country text NOT NULL,
      state text NOT NULL
   );
   """

#GET AND ADD DATA FROM CSV TO SQL TABLES
cursor.execute(create_measure)
data1 = open('clean_measure.csv')
content1 = csv.reader(data1)
insert_data1 = "INSERT INTO measure (station, date, precip, tobs) VALUES (?, ?, ?, ?)"
cursor.executemany(insert_data1, content1)
conn.commit()

cursor.execute(create_stations)
data2 = open('clean_stations.csv')
content2 = csv.reader(data2)
insert_data2 = "INSERT OR REPLACE INTO stations (station, latitude, lognitude, elevation, name, country, state) VALUES (?, ?, ?, ?, ?, ?, ?)"
cursor.executemany(insert_data2, content2)
conn.commit()

#TYPE SQL QUERY HERE BETWEEN '':
query = 'SELECT * FROM stations LIMIT 5'

#SQL QUERY ENGINE
engine = create_engine('sqlite:///database.db')
print(engine.driver)
print(engine.table_names())
print(engine.execute(query))
results = engine.execute(query)
for r in results:
   print(r)

conn.close()