import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            table_name = query.split(' ')[-1]
            print(f'\t-Dropping Table `{table_name}`')
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
            print(query)


def create_tables(cur, conn): 
    for query in create_table_queries:
        try:
            table_name = query.split(' ')[2]
            print(f'\t-Creating Table `{table_name}`')
            cur.execute(query)
            conn.commit()
        except Exception as e:
                print(e)
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

    print('Dropping Existing Tables')
    drop_tables(cur, conn)
    
    print('Creating Tables')
    create_tables(cur, conn)

    
    print('All Tables Created Successfully')
    conn.close()

if __name__ == "__main__":
    main()