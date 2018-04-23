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
    else:
        print("Errro: Missing arguments!")
        sys.exit()

    cluster = get_cluster_by_mode(cluster_mode)

    session = cluster.connect(KEYSPACE)

    start = time.time()
    # Select top 10 movies with the highest revenue
    select_user_query = """
                            SELECT * FROM benchmark.moviesByBudget WHERE type='movie' ORDER BY budget DESC LIMIT 100 ALLOW  FILTERING;
                        """
    # Output
    rows = session.execute(select_user_query)
    print("Title {0:50} Budget".format(""))
    print("-"*64)
    for row in (rows):
        print("{title:50} {:,}".format(row.budget, title=row.title))

    end = time.time()
    session.shutdown()
    print("Time to query ", (end-start))
    session.shutdown()

if __name__ == '__main__':
    main()
