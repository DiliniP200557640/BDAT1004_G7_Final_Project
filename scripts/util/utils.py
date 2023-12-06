"""
Utility functions
"""

import configparser
import psycopg2


# read sql file
def read_sql_file(path, section, param):
    
    config_ini = configparser.ConfigParser()
    config_ini.read(path)
    config = config_ini[section][param]
    return config


# create redshift connection
def create_redshift_conn(**kwargs):
    
    config = kwargs['config']
    print(config)

    try:
        conn_ = psycopg2.connect(dbname=config['db_name'],
                                 host=config['host'],
                                 port=config['port'],
                                 user=config['user'],
                                 password=config['password'])
        return conn_
    except psycopg2.Error as err:
        print(err)
        return None

# run redshift query
def run_redshift_query(rs_conn, query, feed_name, step):
    
    print("\nQuery:")
    print(f"-------\n{query}\n")

    try:
        cur = rs_conn.cursor()
        cur.execute(query)
        rs_conn.commit()

        print(f"{feed_name} - {step} - Query Execution - Successful\n")
        status = 'Success'
        return status
    except psycopg2.Error as error:
        print(f"{feed_name} - {step} - Query Execution - Failed\n")
        print(error)
        rs_conn.rollback()
        status = 'Failed'
        return status