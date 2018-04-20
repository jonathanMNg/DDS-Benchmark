from cassandra.cluster import Cluster

KEYSPACE = 'users'
cluster = Cluster(['35.190.182.110'])

session = cluster.connect('users')

create_table_query = """
                        CREATE TABLE IF NOT EXISTS users.users (
                        userid uuid,
                        username text,
                        firstname text,
                        lastname text,
                        PRIMARY KEY (userid)
                    );
                    """
insert_user_query = """
                        INSERT INTO users.users(userid, username, firstname, lastname)
                        VALUES(uuid(), 'nn4', 'robinson', 'hawke');
                    """
select_user_query = """
                        SELECT * from users.users;
                    """
session.execute(insert_user_query)
rows = session.execute(select_user_query)
for row in rows:
    print(row)
