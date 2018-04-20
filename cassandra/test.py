from cassandra.cluster import Cluster
from functions import *
KEYSPACE = 'users'
cluster = Cluster()

session = cluster.connect()
drop_keyspace(session, KEYSPACE)
create_keyspace(session, KEYSPACE)
session.set_keyspace(KEYSPACE)

create_table_query = """
                        CREATE TABLE IF NOT EXISTS users.users (
                        userid uuid,
                        username text,
                        firstname text,
                        lastname text,
                        PRIMARY KEY (userid)
                    );
                    """
session.execute(create_table_query)

insert_user_query = """
                        INSERT INTO users.users(userid, username, firstname, lastname)
                        VALUES(uuid(), 'nn4', 'robinson', 'hawke');
                    """
session.execute(insert_user_query)

select_user_query = """
                        SELECT * from users.users;
                    """
rows = session.execute(select_user_query)
for row in rows:
    print(row)
