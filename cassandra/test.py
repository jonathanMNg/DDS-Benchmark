from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('benchmark')

session.execute("""
                    CREATE TABLE IF NOT EXISTS testing (uid uuid PRIMARY KEY, name text);
                """)
session.execute("""
                    INSERT INTO testing(uid, name) VALUES(uuid(), 'jonathan');
                """)
rows = session.execute("""
                    SELECT * from testing;
                        """)
for row in rows:
    print(row)
