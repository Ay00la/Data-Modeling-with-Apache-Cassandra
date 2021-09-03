import cassandra
from cassandra.cluster import Cluster

# establish local connection
def local_cassandra():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    return session


# set keyspace
def set_ks(session, query):
    try:
        session.set_keyspace(query)
    except Exception as e:
        print(e)

# execute standard query
def execute_query(session, query):
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    return rows


music_session_insert = '''INSERT INTO music_session
                          (session_id, item_in_session,
                           artist, song_title, song_length)
                           VALUES (%s, %s, %s, %s, %s)'''

