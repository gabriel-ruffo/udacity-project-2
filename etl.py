import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Method: load_staging_tables()
    Parameters: Cursor cur, Connection conn
    Returns: None
    Purpose: Executes and commits the queries to copy data from S3 buckets to their
             respective tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Method: insert_tables()
    Parameters: Cursor cur, Connection conn
    Returns: None
    Purpose: Executes and commits the queries to insert data from the staging
             tables to the star schema'd tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()   
    

def main():
    """
    Method: main()
    Parameters: None
    Returns: None
    Purpose: Reads the config file to connect to the database hosted in Redshift, then loads the staging
             tables with the data stored in S3 buckets. The data is then manipulated and inserted into
             the star schema'd tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Attempting to connect...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Connected!")
    
    start_time = time.time()
    print("Starting timing...")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    end_time = time.time()
    print("Table creating and insertion took: {} seconds.".format(end_time-start_time))
    
    conn.close()


if __name__ == "__main__":
    main()