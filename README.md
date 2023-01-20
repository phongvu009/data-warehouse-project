# Sparkify Database

## overview

Migrating data to the cloud . When the data is ready on resshift, it will provide instantly queyring for analytic business

## architecture

There are 4 Dimension Tables:

- users: store all user information such as name , gender and level
- songs: store song information such as song name, artist, year, duration
- artists: store artist information such as name, base location
- time: store timestamps of records

One fact table:

- songplays: store information of a session by user

## Examples of queries

1. What is the top 10 most played song
   This will give the top 10 most played song name
2. Give me the top 10 songs played in certain year
   This will list the most played songs in certian year
3. what is the most played song by certain user
   This will give the list most listening song by an user

## How to run 
1. Create tables
'''
python create_tables.py
'''
2. Do ETL 
'''
python etl.py
'''
