import os
import pandas as pd
import psycopg2

# set up the connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="sailing",
    user="--",
    password="--"
)
cursor = conn.cursor()

# create a table to store the gps data
cursor.execute('''CREATE TABLE IF NOT EXISTS danielamexico
                  (id SERIAL PRIMARY KEY,
                   filename TEXT,
                   timestamp timestamp,
                   lat numeric,
                   lon numeric,
                   sog numeric,
                   cog numeric,
                   hdg numeric,
                   roll numeric,
                   pitch numeric
                   )''')

# set up the path to the desktop folder
desktop_path = os.path.expanduser("~/Desktop/daniela/")

# loop through all the files in the desktop folder
for file in os.listdir(desktop_path):
    if file.endswith(".csv"):  # assumes only CSV files are being imported
        filepath = os.path.join(desktop_path, file)
        df = pd.read_csv(filepath)
        # insert the data into the PostgreSQL database
        for row in df.itertuples():
            cursor.execute('''INSERT INTO danielamexico (filename, timestamp, lat, lon, sog, cog, hdg, roll, pitch)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', (file, row.timestamp, row.latitude, row.longitude, row.sog_kts, row.cog, row.hdg_true, row.roll, row.pitch))

# create table to store kite sizes and foils used on each file
cursor.execute('''CREATE TABLE IF NOT EXISTS session_index 
                  (session_name TEXT PRIMARY KEY,
                   foil_serial_numbers TEXT,
                   finish TEXT,
                   kite_serial_number TEXT,
                   size SMALLINT,
                   addl_notes TEXT,
                   files TEXT,
                   foil TEXT,
                   conditions TEXT,
                   power TEXT
                   )''')

# Load data from csv into a pandas dataframe
df = pd.read_csv("~/Desktop/session_index.csv")

# Replace null values with None
df = df.where(pd.notnull(df), None)

# insert the data into the PostgreSQL database
for row in df.itertuples():
    cursor.execute('''INSERT INTO session_index (session_name, foil_serial_numbers, finish, kite_serial_number, size, addl_notes, files, foil, conditions, power)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                  (row.session_name, row.foil_serial_numbers, row.finish, row.kite_serial_number, row.size, row.addl_notes, row.files, row.foil, row.notable_conditions, row.power))

# alter gps table in order to add new data from session index
cursor.execute('''ALTER TABLE danielamexico
            ADD COLUMN IF NOT EXISTS wing VARCHAR,
            ADD COLUMN IF NOT EXISTS tail VARCHAR,
            ADD COLUMN IF NOT EXISTS mast VARCHAR,
            ADD COLUMN IF NOT EXISTS kite_size VARCHAR''') 

# correct some typos in the filenames that resulted from downloading
cursor.execute('''UPDATE session_idex
                    SET files = REPLACE(files, '_', ''),
                    SET files = REPLACE(files, ' ', '_'),
                    SET files = REPLACE(files, '#', '')''')

# match wing, tail, and mast serial numbers to danielamexico based on filename and update table
cursor.execute('''WITH foils as (
                    SELECT DISTINCT(mytable.filename),
                        SPLIT_PART(session_index.foil_serial_numbers, ',',1) AS wing,
                        SPLIT_PART(session_index.foil_serial_numbers,',',2) AS tail,
                        SPLIT_PART(session_index.foil_serial_numbers,',',3) AS mast
                    FROM mytable
                    LEFT JOIN session_index
                        ON mytable.filename LIKE session_index.files)

                    UPDATE mytable
                    SET wing = foils.wing, 
                        tail = foils.tail, 
                        mast = foils.mast
                    FROM foils
                    WHERE mytable.filename = foils.filename''')

# add kite size to danielamexico based on filename 
cursor.execute('''UPDATE danielamexico
                SET kite_size=session_index.size
                FROM session_index
                WHERE danielamexico.filename = session_index.files''')


# commit the changes to the PostgreSQL database and close the connection
conn.commit()
conn.close()
