''' To load XML file into a database with 4 tables '''

import xml.etree.ElementTree as ET
import sqlite3

# ***  STEP 1: create a database
con = sqlite3.connect('music_track.sqlite')
cursor = con.cursor()

# ***  STEP 2: create 4 relational tables, 'Tracks' table has 3 foreign keys refered to 3 tables
# Album table has a foreign key of artist_id from Artist table
# "UNIQUE" is added in lines to make sure 'INSERT OR IGNORE' statment can be used to only
# add in unique titles or names into the tables.
cursor.executescript('''
    drop table if exists Album;
    drop table if exists Artist;
    drop table if exists Tracks;
    drop table if exists Genre;

    create table if not exists Album (
        id integer primary key autoincrement not null unique,
        artist_id integer,
        title text unique);
    create table if not exists Artist (
        id integer primary key autoincrement not null unique,
        name text unique);
    create table if not exists Genre (
        id integer primary key autoincrement not null unique,
        name text unique);
    create table if not exists Tracks (
        id integer primary key autoincrement not null unique,
        name text unique,
        artist_id integer,
        album_id integer,
        genre_id integer,
        rating integer,
        len real,
        playCount integer,
        FOREIGN KEY (artist_id)
        REFERENCES Artist (id),
        FOREIGN KEY (album_id)
        REFERENCES Album (id),
        FOREIGN KEY (genre_id)
        REFERENCES Genre (id)
    );
''')

# get all 'dict' in 3rd level from xml file
tree = ET.parse('library.xml')
root = tree.getroot()

# find all 'dict' element in the deepest level
content = root.findall('dict/dict/dict') 

# A helper method for grabing 'text' of a tag following the right 'key' tag,
# only when find the right 'key' tag, then return the following tag.text
def checkup(dict, keyword):
    found = False
    for child in dict:
        if found: # if found the right 'key' tag
            return child.text
        # find the right 'key' tag, then claim 'found'
        elif child.tag == 'key' and child.text == keyword:
            found = True
    return None


# ***  STEP 3: fill up the database with the right data
for child in content:
    #  1: get all variables ready
    name = checkup(child, 'Name')
    artist = checkup(child, 'Artist')
    album = checkup(child, 'Album')
    # format time 1) from millisecond to mins 2) show only 2 decimal places 3) add ' mins'
    total_time_origin = checkup(child,'Total Time')
    if total_time_origin:
        total_time = '%.2f' % ((int(total_time_origin)) / 60000.0) + ' mins'
    elif total_time_origin is None: # only one track has no value for 'Total Time'
        total_time = '0 mins'
    play_count = checkup(child, 'Play Count')
    rating = checkup(child, 'Rating')
    genre = checkup(child,'Genre')
    # print(name, artist, album, track_number, play_count, rating, genre)

    if name is None or artist is None or album is None or genre is None:
        continue

    
    #    2: add info into the database
    # => 1). insert only unique value into table
    # => 2). get the id value of this newly inserted row as foreign key for Tracks and Album tables

    cursor.execute('''
    insert or ignore into Artist (name) values (?);
    ''',(artist,))
    cursor.execute('''select id from Artist where name == (?);''', (artist,))
    artist_id = cursor.fetchone()[0] # [0] => from tuple (1,) to 1

    # Album table needs Artist table id for foreign key
    cursor.execute('''
    insert or ignore into Album (artist_id, title) values (?,?);
    ''',(artist_id, album))
    cursor.execute('''select id from Album where title == (?);''', (album,))
    album_id = cursor.fetchone()[0]     

    cursor.execute('''
    insert or ignore into Genre (name) values (?);
    ''',(genre,))
    cursor.execute('''select id from Genre where name == (?);''', (genre,))
    genre_id = cursor.fetchone()[0]

    ''' The REPLACE command in SQLite is an alias for the INSERT OR REPLACE command.
    when any UNIQUE or PRIMARY Key constraint is violated then the REPLACE command will 
    delete the row that causes constraint violation and insert a new row.'''

    # with 3 foreign key
    # s collected, now insert data into Tracks table    
    cursor.execute('''
    insert or replace into Tracks (name, artist_id, album_id, genre_id, rating, len, playCount) 
    values (?,?,?,?,?,?,?);
    ''',(name,artist_id,album_id, genre_id, rating, total_time,play_count))


# ***  STEP 4: Join tables for useful data display
cursor.execute('''
select Tracks.name as TrackName, Album.title as Album, Artist.name as Artist, Genre.name as Genre 
from Tracks 
Join Album
Join Artist
Join Genre
On Tracks.album_id = Album.id 
AND Tracks.artist_id = Artist.id
AND Tracks.genre_id = Genre.id;
''')
print(cursor.fetchall())

con.commit()
con.close()





