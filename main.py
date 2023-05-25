from dotenv import load_dotenv
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import pandas as pd
import pymysql

# load environment variables
load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = os.getenv('SPOTIFY_REDIRECT_URI')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')

# create spotify object
spotify = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
        redirect_uri=SPOTIFY_REDIRECT_URI,
        scope = 'user-read-recently-played'
    )
)

# get most recent played tracks on spotify (limit=50)
data = spotify.current_user_recently_played()
last_50_items = data['items']

last_50_tracks = []
last_50_artists = []
last_50_albums = []
last_50_played_at = []

for item in last_50_items:
    last_50_tracks.append(item['track']['name'])
    last_50_artists.append(item['track']['artists'][0]['name'])
    last_50_albums.append(item['track']['album']['name'])
    last_50_played_at.append(item['played_at'].replace("T", " ").split(".")[0])

# create data dictionary
my_data = {
    "tracks": last_50_tracks,
    "artists": last_50_artists,
    "albums": last_50_albums,
    "played_at": last_50_played_at,
}

# write data to json file
with open("data.json", "w") as file:
    json.dump(my_data, file, indent=4)

# convert data to DataFrame
df = pd.DataFrame.from_dict(my_data)
df['played_at'] = pd.to_datetime(df['played_at'])

# design new database and load data into database
try:
    conn = pymysql.connect(
        host='localhost',
        user='quincytromp',
        password=DATABASE_PASSWORD,
        autocommit=True
    )
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

try:
    cur = conn.cursor()
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

try:
    cur.execute('''
DROP DATABASE IF EXISTS spotify; 
''')
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

try:
    cur.execute('''
CREATE DATABASE spotify; 
''')
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

try:
    cur.execute('''
USE spotify; 
''')
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

try:
    cur.execute('''
CREATE TABLE played_tracks (
    played_at datetime PRIMARY KEY,
    track varchar(256),
    artist varchar(256),
    album varchar(256));''')
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

for i,row in df.iterrows():
    try:
        cur.execute('''
    INSERT INTO played_tracks (
        track, artist, album, played_at)
        VALUES (%s, %s, %s, %s);''', tuple(row))
    except (pymysql.Error, pymysql.Warning) as e:
        print(e)

try:
    cur.execute('''
SELECT * FROM played_tracks;    
''')
except (pymysql.Error, pymysql.Warning) as e:
    print(e)

# validate data
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

try:
    cur.close()
    conn.close()
except (pymysql.Error, pymysql.Warning) as e:
    print(e)
print("Finished")