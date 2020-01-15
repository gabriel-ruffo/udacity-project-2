import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Method: drop_tables()
    Parameters: Cursor cur, Connectioon conn
    Returns: None
    Purpose: Executes and commits the queries that drop the tables in the database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Method: create_tables()
    Parameters: Cursor cur, Connection conn
    Returns: None
    Purpose: Executes and commits the queries that create the tables in the database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Method: main()
    Parameters: None
    Return: None
    Purpose: main() method called to start the process of deleting and creating the tables in the
             database. Calls ConfigParser() to read the config file's values to open a connection
             to the database. Then drops and creates the tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Attempting to connect...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected!")
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()