import psycopg2
from  psycopg2 import ProgrammingError, DatabaseError
import logging
import pandas as pd
from io import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Connection(object):
    HOST='127.0.0.1'   
    USER='student'
    PASSWORD='student'
    
    def __init__(self, database_name:str):
        self.database_name = database_name
           
    def establish_connection(self, auto_commit:bool = True):
        conn = psycopg2.connect(f"host={self.HOST} dbname={self.database_name} user={self.USER} password={self.PASSWORD}")
        conn.set_session(autocommit=auto_commit)
        logger.info(f"Connection Established to: {self.database_name}")
        return conn

class Postgre(object):
    
    def __init__(self, database_name:str):
        self.connection = Connection(database_name)
        self.conn = self.connection.establish_connection()
    
    @staticmethod
    def _get_results_dict(column_names:list, list_records:list):
        return [{key:value for key, value in zip(column_names, entry)} for entry in list_records] 
    
    def close_connectiion(self):
        if self.conn:
            logger.info('Closing Connection')
            self.conn.close()
        

    def _execute_query(self, query:str, results:bool = False):    
        try:
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            if results:
                return self._get_results_dict([desc[0] for desc in cursor.description], cursor.fetchall())
            else:
                return True
        except ProgrammingError:
            logger.exception("Error commiting the results")
            return False
        except DatabaseError:
            logger.exception("Database Error")
            return False
        except:
            logger.exception(f"Query Execution was not successful")
            return False
            
            
            
    def create_database(self, database_name:str):
        query = f"""DROP DATABASE IF EXISTS {database_name};"""
        result = self._execute_query(query)
        logger.info(f"Database {database_name} dropped: {result}")
        query= f"""CREATE DATABASE {database_name} WITH ENCODING 'utf8' TEMPLATE template0;"""
        result = self._execute_query(query)
        logger.info(f"Database {database_name} created: {result}")
        
    
    def drop_table(self, table_name:str):
        query = f"""DROP TABLE IF EXISTS {table_name};"""     
        result = self._execute_query(query)
        logger.info(f"Table {table_name} dropped: {result}")
    
    def create_table(self, table_name:str, columns_dict:dict):
        self.drop_table(table_name)
        columns_statement = ',\n\t'.join([f"{key} {value}" for key, value in columns_dict.items()])
        query = f"""
                CREATE TABLE {table_name} (
                \t{columns_statement});
                """.replace("  ", "")
        
        result = self._execute_query(query)
        logger.info(f"Table {table_name} created: {result}")
        
    def copy_to_table(self, file_object, table_name:str, columns:list, sep:str='\t', null_value:str=''):
        file_object.seek(0)
        number_records = len([entry for entry in file_object.getvalue().split('\n') if entry])
        cursor = self.conn.cursor()
        cursor.copy_from(file_object, table=table_name, columns=columns, sep=sep, null=null_value)
        logger.info(f"Inserted {number_records} Records to Table {table_name}")
    
    def copy_dataframe_to_table(self, df: pd.DataFrame, table_name:str):
        file_object = StringIO()
        column_names = list(df.columns)
        df.to_csv(file_object, columns=column_names, sep='\t', header=False, index=False)
        self.copy_to_table(file_object, table_name, column_names)
    
    def insert_records(self, table_name:str, records:list):
        
        result = self._execute_query(query)
        logger.info(f"Table {table_name} created: {result}")
        
    
    def delete_records(self, table_name:str, records:list):
        result = self._execute_query(query)
        logger.info(f"Table {table_name} created: {result}")
            



