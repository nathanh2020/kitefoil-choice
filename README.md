# kitefoil-choice

6x kitefoil world champion Daniela Moroz is training in Mexico. The purpose of her training: test different foils.
However, after a month of collecting data she finds that her wind sensor was broken all along.
Without wind data, specifically wind direction it is difficult to compare the data from one day to the data from another.

This repository shows my approach to solving that problem.

## sql_file_import.py

This file imports all the data from the month of training as .csv files into my postgresql database.

## weather_conn.py

This file connects to _Visual Crossing's_ Weather API.
It queries the api for relevant wind data including direction, gusts, and speed.
It joins that data back to Daniela's kiting data in my sql database and exports that as a .csv file for further analysis and visualization in Tableau.
