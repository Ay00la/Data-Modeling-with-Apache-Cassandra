import pandas as pd
import os
import glob
import csv
from helpers import create_row_list, write_output_csv
from db.cdb import local_cassandra, execute_query, set_ks

# checking current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    # join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)

# create empty list
full_data_rows_list = []

# add rows to empty list
create_row_list(file_path_list, full_data_rows_list)

# define headers and write rows to output file
headers = ['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId']
write_output_csv('event_datafile_new.csv', headers, full_data_rows_list)

# establish cassandra connection
conn = local_cassandra()

# define keyspace
keyspace = """sparkify_k"""

# Create a Keyspace 
query = """CREATE KEYSPACE IF NOT EXISTS sparkify_k
        WITH REPLICATION =
        {'class' : 'SimpleStrategy', 'replication_factor' : 1}"""

# create keyspace
execute_query(conn, query)

# Set keyspace
set_ks(conn, keyspace)

# Query 1: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, \
# and itemInSession = 4
query = '''CREATE TABLE IF NOT EXISTS music_session (
        session_id INT, item_in_session INT, artist TEXT, song_title TEXT, song_length FLOAT,
        PRIMARY KEY(session_id, item_in_session))'''

execute_query(conn, query)

file = 'event_datafile_new.csv'

with open(file, 'r', encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    
    for line in csvreader:

        # Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_session (session_id, item_in_session, artist, song_title, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        
        # Assign which column element should be assigned for each column in the INSERT statement.
        try:
            conn.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
        except Exception as e:
            print(e)

# Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) \
# for userid = 10, sessionid = 182
query = '''CREATE TABLE IF NOT EXISTS event_log (
        user_id INT, session_id INT, item_in_session INT,
        artist TEXT, song_title TEXT,
        first_name TEXT, last_name TEXT,
        PRIMARY KEY (user_id, session_id, item_in_session))'''

execute_query(conn, query)

file = 'event_datafile_new.csv'

with open(file, 'r', encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    
    for line in csvreader:                
        query = "INSERT INTO event_log (user_id, session_id, item_in_session, artist, song_title, first_name, last_name)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        # Assign which column element should be assigned for each column in the INSERT statement.
        try:
            conn.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
        except Exception as e:
            print(e)

query = '''SELECT * FROM event_log WHERE user_id = 10 AND session_id = 182'''

rows = execute_query(conn, query)
for i in rows:
    print(i)

# Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = """CREATE TABLE IF NOT EXISTS song_users (song_title TEXT, user_id INT, first_name TEXT, last_name TEXT,
        PRIMARY KEY(song_title, user_id))"""
try:
    conn.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

# Read csv file with pandas 
df = pd.read_csv(file, usecols = [9, 10, 1, 4])
df.drop_duplicates(inplace=True)

for i, row in df.iterrows():
    
    query = "INSERT INTO song_users (song_title, user_id, first_name, last_name)"
    query = query + "VALUES (%s, %s, %s, %s)"

    try:
        conn.execute(query, (row.song, int(row.userId), row.firstName, row.lastName))
    except Exception as e:
        print(e)

query = """SELECT * FROM song_users WHERE song_title = 'All Hands Against His Own'"""

rows = execute_query(conn, query)
for i in rows:
    print(i)

# Drop the tables before closing out the sessions
drop_musicsession = "DROP TABLE IF EXISTS music_session"
drop_eventlog = "DROP TABLE IF EXISTS event_log"
drop_songusers = "DROP TABLE IF EXISTS song_users"

execute_query(conn, drop_musicsession)
execute_query(conn, drop_eventlog)
execute_query(conn, drop_songusers)

# shutdown connection
conn.shutdown()
