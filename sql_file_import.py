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

# create a table to store the data
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

# commit the changes to the PostgreSQL database and close the connection
conn.commit()
conn.close()
