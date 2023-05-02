import requests as r
from sqlalchemy import create_engine
import pandas as pd


# create an engine using your database URL
engine = create_engine('postgresql://user:password@ip-address:port/database')

# to minimize api queries I am querying only wind data for a random gps point on every odd minute
data_frame= pd.read_sql_query("SELECT DISTINCT ON (date_trunc('minute', timestamp)) concat(lat,',',lon) AS latlon, "
                              "date_trunc('minute',timestamp) AS datemin "
                              "FROM danielamexico "
                              "WHERE EXTRACT(MINUTE FROM timestamp) % 2 = 1 "
                              "ORDER BY datemin ASC, RANDOM()", con=engine)

#connect to visual crossing's weather api and query for each row in my data frame
api_key= '--'

#query visual crossing weather data api for each data point in the data frame
for i in range(0,len(data_frame),1):
    location= str(data_frame.loc[i,'latlon'])
    date = str(data_frame.loc[i,'datemin'])
    url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'+location+'/'+date.replace(' ','T')+'?key='+api_key+'&include=current'

    request = r.get(url)
    json = request.json()

    #get wind direction, gust, and windspeed from the json
    winddir = json.get('currentConditions').get('winddir')
    windgust= json.get('currentConditions').get('windgust')
    windspeed= json.get('currentConditions').get('windspeed')

    #commit this data to each row of my dataframe
    data_frame.loc[i,'winddir'] = winddir
    data_frame.loc[i,'windgust']= windgust
    data_frame.loc[i,'windspeed']= windspeed

#write that data frame back into my postgressql database with the name winddir
data_frame.to_sql('winddir', engine, if_exists='replace', index=False)

#join wind data to kiting gps and accelerometer data
data_frame= pd.read_sql_query("SELECT a.id, a.filename, a.timestamp, a.lat, a.lon, a.sog, a.cog, a.hdg, "
                              "a.roll, a.pitch, a.wing, a.tail, a.mast, a.kite_size, b.winddir, b.windgust, b.windspeed "
                              "FROM danielamexico a "
                              "LEFT JOIN winddir b "
                              "ON date_trunc('minute', a.timestamp) = b.datemin "
                              "WHERE b.datemin IS NOT NULL", con=engine)

#write that data to a csv on my desktop for use in tableau
data_frame.to_csv('/Users/nathanhousberg/Desktop/foil_testing.csv',index= False)


#commit connection and close
engine.connect().commit()
engine.dispose()
