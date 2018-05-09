from cassandra.cluster import Cluster
from functions import *
from cassandra import ConsistencyLevel
from datetime import datetime
import time, sys
from uuid import uuid1


KEYSPACE = 'benchmark'
def get_cluster_by_mode(cluster_mode):
    cluster = None
    if (cluster_mode == 'localhost'):
        cluster = Cluster()
    elif(cluster_mode == 'single'):
        cluster = Cluster(['10.142.0.4'])
    elif(cluster_mode == 'cluster'):
        cluster = Cluster(['10.142.0.2', '10.142.0.3', '10.142.0.5'])
    else:
        print("Error: Invalid mode!")
        sys.exit()
    return cluster
def main():
    if(len(sys.argv) >= 1):
        cluster_mode = sys.argv[1]
    else:
        print("Errro: Missing arguments!")
        sys.exit()

    cluster = get_cluster_by_mode(cluster_mode)

    create_table_movies_byRevenue_query = """
                                    CREATE TABLE IF NOT EXISTS {keyspace}.moviesByRevenue (
                                    uid uuid,
                                    type text,
                                    id int,
                                    title text,
                                    popularity float,
                                    vote_average float,
                                    budget varint,
                                    revenue varint,
                                    release_date timestamp,
                                    PRIMARY KEY (type, revenue, uid)
                                    );
                                """ .format(keyspace=KEYSPACE)
    drop_table_movies_byRevenue_query = """
                                DROP TABLE IF EXISTS benchmark.moviesByRevenue;
                              """
    create_table_movies_byBudget_query = """
                                    CREATE TABLE IF NOT EXISTS {keyspace}.moviesByBudget (
                                    uid uuid,
                                    type text,
                                    id int,
                                    title text,
                                    popularity float,
                                    vote_average float,
                                    budget varint,
                                    revenue varint,
                                    release_date timestamp,
                                    PRIMARY KEY (type, budget, uid)
                                    );
                                """ .format(keyspace=KEYSPACE)
    drop_table_movies_byBudget_query = """
                                DROP TABLE IF EXISTS benchmark.moviesByBudget;
                              """
    start_time = time.time()
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
    end_time = time.time()

    total_runtime = end_time - start_time
    print("Total runtime: ", total_runtime)

if __name__ == '__main__':
    main()
