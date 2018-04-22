"""
This file contains the functions for cassandra system
"""


import csv


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
                            CREATE KEYSPACE IF NOT EXISTS %s WITH replication =
                            { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                            """ % KEYSPACE)
    return True


"""
function:
    name: parse_CSV
    parameter: filename, delimiter
    return
"""
def parse_CSV(filename, csv_delimiter = ','):
    csvData = []
    with open(filename, newline='' ) as f:
        reader = csv.reader(f, delimiter=csv_delimiter)
        for row in reader:
            if(row[-1] == ''):
                row = row[:-1]
            csvData.append(row)
    return csvData

"""
function:
    name: get_safe_string
    parameter: string
    return None null string
"""
def get_safe_string(string):
    if not string:
        return 0
    else:
        return string
"""
function:
    name: get_safe_string
    parameter: string
    return None null string
"""
def get_safe_date(string):
    if not string:
        return "1111-11-11"
    else:
        return string
