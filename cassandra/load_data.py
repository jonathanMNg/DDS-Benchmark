from cassandra.cluster import Cluster
from functions import *
from cassandra import ConsistencyLevel
from datetime import datetime
import time, sys


KEYSPACE = 'benchmark'
def get_cluster_by_mode(cluster_mode):
    cluster = None
    if (cluster_mode == 'localhost'):
        cluster = Cluster()
    elif(cluster_mode == 'single'):
        cluster = Cluster(['10.142.0.4'])
    elif(cluster_mode == 'cluster'):
        cluster = Cluster(['10.142.0.2', '10.142.0.3'])
    else:
        print("Error: Invalid mode!")
        sys.exit()
    return cluster
def main():
    if(len(sys.argv) >= 2):
        cluster_mode = sys.argv[1]
        filename = sys.argv[2]
    else:
        print("Errro: Missing arguments!")
        sys.exit()

    cluster = get_cluster_by_mode(cluster_mode)
    csv_data = parse_CSV(filename)

    csv_header = csv_data.pop(0)

    movies_table = []

    for row in csv_data:
        dict_row = {}
        for i, col_name in enumerate(csv_header):
            dict_row[col_name] = row[i]
        movies_table.append(dict_row)

    create_table_movies_byRevenue_query = """
                                    CREATE TABLE IF NOT EXISTS {keyspace}.moviesByRevenue (
                                    type text,
                                    id int,
                                    title text,
                                    popularity float,
                                    vote_average float,
                                    budget varint,
                                    revenue varint,
                                    release_date timestamp,
                                    PRIMARY KEY (type, revenue)
                                    );
                                """ .format(keyspace=KEYSPACE)
    drop_table_movies_byRevenue_query = """
                                DROP TABLE IF EXISTS benchmark.moviesByRevenue;
                              """
    create_table_movies_byBudget_query = """
                                    CREATE TABLE IF NOT EXISTS {keyspace}.moviesByBudget (
                                    type text,
                                    id int,
                                    title text,
                                    popularity float,
                                    vote_average float,
                                    budget varint,
                                    revenue varint,
                                    release_date timestamp,
                                    PRIMARY KEY (type, budget)
                                    );
                                """ .format(keyspace=KEYSPACE)
    drop_table_movies_byBudget_query = """
                                DROP TABLE IF EXISTS benchmark.moviesByBudget;
                              """
    insert_movie_byRevenue_query = """
                            INSERT INTO {keyspace}.moviesByRevenue ( type, id, title, popularity, vote_average, budget, revenue, release_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                         """ .format(keyspace=KEYSPACE)
    insert_movie_byBudget_query = """
                            INSERT INTO {keyspace}.moviesByBudget ( type, id, title, popularity, vote_average, budget, revenue, release_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                         """ .format(keyspace=KEYSPACE)

    session = cluster.connect()
    print("Processing keyspace `%s`..." % KEYSPACE)
    new_keyspace = create_keyspace(session, KEYSPACE)
    if(not new_keyspace):
        print("Keyspace `%s` is valid and ready to use..." % KEYSPACE)
    else:
        print("Keyspace `%s` is successful created..." % KEYSPACE)
    print("Set `%s` as keyspace" % KEYSPACE)
    session.set_keyspace(KEYSPACE)

    print("Initing table...")
    #byBudget
    session.execute(drop_table_movies_byRevenue_query)
    session.execute(create_table_movies_byRevenue_query)
    #byRevenue
    session.execute(drop_table_movies_byBudget_query)
    session.execute(create_table_movies_byBudget_query)
    print("Preparing query to insert...")
    prepared_insert_movie_byRevenue_query = session.prepare(insert_movie_byRevenue_query)
    prepared_insert_movie_byRevenue_query.consistency_level = ConsistencyLevel.ONE

    prepared_insert_movie_byBudget_query = session.prepare(insert_movie_byBudget_query)
    prepared_insert_movie_byBudget_query.consistency_level = ConsistencyLevel.ONE
    print("Inserting movies...")
    start_insert_movies = time.time()
    for movie in movies_table:
        bound_byRevenue = (prepared_insert_movie_byRevenue_query.bind((
                                                     'movie',
                                                     int(movie['id']),
                                                     movie['title'],
                                                     float(movie['popularity']),
                                                     float(movie['vote_average']),
                                                     int(movie['budget']),
                                                     int(movie['revenue']),
                                                     datetime.strptime(get_safe_date(movie['release_date']),'%Y-%m-%d'))))
        bound_byBudget = (prepared_insert_movie_byBudget_query.bind((
                                                     'movie',
                                                     int(movie['id']),
                                                     movie['title'],
                                                     float(movie['popularity']),
                                                     float(movie['vote_average']),
                                                     int(movie['budget']),
                                                     int(movie['revenue']),
                                                     datetime.strptime(get_safe_date(movie['release_date']),'%Y-%m-%d'))))
        session.execute(bound_byRevenue)
        session.execute(bound_byBudget)

    end_insert_movies = time.time()
    print("Time to insert: %s" % (end_insert_movies - start_insert_movies))
    session.shutdown()

if __name__ == '__main__':
    main()
