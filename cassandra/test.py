from cassandra.cluster import Cluster
from functions import *
import sys
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from datetime import datetime
import time

KEYSPACE = 'benchmark'
cluster = Cluster()

filename = sys.argv[1]
csv_data = parse_CSV(filename)

csv_header = csv_data.pop(0)

movies_table = []

for row in csv_data:
    dict_row = {}
    for i, col_name in enumerate(csv_header):
        dict_row[col_name] = row[i]
    movies_table.append(dict_row)
create_movies_table_query = """
                                CREATE TABLE IF NOT EXISTS benchmark.movies (
                                type text,
                                id int,
                                title text,
                                popularity float,
                                vote_average float,
                                runtime int,
                                budget varint,
                                revenue varint,
                                release_date timestamp,
                                PRIMARY KEY ((type), revenue)
                                ) WITH CLUSTERING ORDER BY (revenue DESC);
                            """
insert_movie_query = """
                        INSERT INTO benchmark.movies (type, id, title, popularity, vote_average, runtime, budget, revenue, release_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                     """
insert_movie_simple_query = SimpleStatement(insert_movie_query, consistency_level=ConsistencyLevel.ONE)

session = cluster.connect()
#drop_keyspace(session, KEYSPACE)
#create_keyspace(session, KEYSPACE)

session.set_keyspace(KEYSPACE)

# Movies data processing start
#session.execute(create_movies_table_query)
"""
for movie in movies_table:
    session.execute(insert_movie_simple_query, (
                                                 'movie',
                                                 int(movie['id']),
                                                 movie['title'],
                                                 float(movie['popularity']),
                                                 float(movie['vote_average']),
                                                 int(get_safe_string(movie['runtime'].split('.')[0])),
                                                 int(movie['budget']),
                                                 int(movie['revenue']),
                                                 movie['release_date']))
"""
start = time.time()
select_user_query = """
                        SELECT * FROM benchmark.movies WHERE type='movie' ORDER BY revenue DESC LIMIT 5000;
                    """

rows = session.execute(select_user_query)
for row in (rows):
    print("{title:50} {:,}".format(row.revenue, title=row.title))

end = time.time()
print(end - start)
