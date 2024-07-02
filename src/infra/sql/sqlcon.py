from dotenv import load_dotenv
import pyodbc
import pandas
import os
class sqlcon:
    def __init__(self):
        load_dotenv()
        self.server = os.getenv('memphis_server')
        self.database = os.getenv('memphis_database')
        self.user_name = os.getenv('memphis_user')
        self.password = os.getenv('memphis_pass')
    def create_connect(self):
        connectionString = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.user_name};PWD={self.password}'
        self.conn = pyodbc.connect(connectionString)
        self.conn.autocommit = False
    def execute_query(self, query):
        self.create_connect()
        df = pandas.read_sql_query(query, self.conn)
        self.conn.close()
        return df
