import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            table_name = query.split(' ')[1]
            print(f'\t-Loading Staging Table `{table_name}`')
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(query)
            raise e


def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            table_name = query.split(' ')[2]
            print(f'\t-Inserting Into Analytics Table `{table_name}`')
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(query)
            raise e


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    print('Connecting to Redshift')
    conn = psycopg2.connect("host={HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}".format(
        HOST=config['CLUSTER'].get('HOST'),
        DB_NAME=config['CLUSTER'].get('DB_NAME'),
        DB_USER=config['CLUSTER'].get('DB_USER'),
        DB_PASSWORD=config['CLUSTER'].get('DB_PASSWORD'),
        DB_PORT=config['CLUSTER'].get('DB_PORT'))
        )
    print('Connected Succesfully to Redshift')
    cur = conn.cursor()
    
    print('Sparkify ETL Starting')
    print('Loading Staging Tables from S3 to Redshift')
    load_staging_tables(cur, conn)
    
    print('Transform Tables From Staging to Analytics Format')
    insert_tables(cur, conn)

    conn.close()
    print('Sparkify ETL Ended')


if __name__ == "__main__":
    main()
    
