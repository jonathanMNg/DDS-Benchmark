from cassandra.cluster import Cluster
from functions import *
from cassandra import ConsistencyLevel
from datetime import datetime
import time, sys

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

create_table_movies_query = """
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
                                PRIMARY KEY (type, revenue)
                                );
                            """
drop_table_movies_query = """
                            DROP TABLE IF EXISTS benchmark.movies;
                          """
insert_movie_query = """
                        INSERT INTO benchmark.movies ( type, id, title, popularity, vote_average, runtime, budget, revenue, release_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS;
                     """
print("Trying to connect....")
session = cluster.connect()
new_keyspace = create_keyspace(session, KEYSPACE)
print("set key space..")
session.set_keyspace(KEYSPACE)

# Movies data processing start
session.execute(create_table_movies_query)
#print("Trying to query....")
prepared_insert_movie_query = session.prepare(insert_movie_query)
prepared_insert_movie_query.consistency_level = ConsistencyLevel.LOCAL_ONE
it = 0
print("Inserting movies...")
for movie in movies_table:
    it += 1
    rs = session.execute(prepared_insert_movie_query, (
                                                 'movie',
                                                 int(movie['id']) + it,
                                                 movie['title'],
                                                 float(movie['popularity']),
                                                 float(movie['vote_average']),
                                                 int(get_safe_string(movie['runtime'].split('.')[0])),
                                                 int(movie['budget']),
                                                 int(movie['revenue']),
                                                 datetime.strptime(get_safe_date(movie['release_date']),
                                                 '%Y-%m-%d')))
print("Finish insert movies!", it)

start = time.time()
# Select top 10 movies with the highest revenue
select_user_query = """
                        SELECT * FROM benchmark.movies WHERE type='movie' AND release_date > '2011-01-01' AND release_date < '2011-02-01' ORDER BY revenue DESC LIMIT 100 ALLOW  FILTERING;
                    """
# Output

it = 0
rows = session.execute(select_user_query)
for row in (rows):
    print("{title:50} {:,}".format(row.revenue, title=row.title))

end = time.time()
print ("result ", it)
session.shutdown()
print("Time to query ", (end-start))
"""
plt.plot([1,2,3,4,5], timing, 'ro')
plt.axis([0, 6, 0, 6])
plt.grid(color='g', linestyle='-', linewidth=0.5)
plt.show()
"""
