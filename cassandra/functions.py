"""
function:
    name: drop_keyspace
    parameter:  KEYSPACE, cluster_session
    return True if KEYSPACE exists, otherwise return False
"""
def drop_keyspace(cluster_session, KEYSPACE):
    rows = cluster_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        cluster_session.execute("DROP KEYSPACE " + KEYSPACE)
        return True
    return False
"""
function:
    name: create_keyspace
    parameter:  KEYSPACE, cluster_session
    return True if KEYSPACE exists, otherwise return False
"""
def create_keyspace(cluster_session, KEYSPACE):
    rows = cluster_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
    if KEYSPACE in [row[0] for row in rows]:
        return False
    cluster_session.execute("""
                            CREATE KEYSPACE %s WITH replication =
                            { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                            """ % KEYSPACE)
    return True
