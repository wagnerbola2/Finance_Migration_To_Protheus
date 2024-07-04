from dotenv import load_dotenv
import pyodbc
import pandas
import os
class sqlcon:
    def __init__(self):
        load_dotenv()
        self.server = os.getenv('SERVER')
        self.database = os.getenv('DATABASE')
        self.user_name = os.getenv('USER')
        self.password = os.getenv('PASS')
    def create_connect(self):
        connectionString = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.user_name};PWD={self.password}'
        self.conn = pyodbc.connect(connectionString)
        self.conn.autocommit = False
    def execute_query(self, query):
        self.create_connect()
        df = pandas.read_sql_query(query, self.conn)
        self.conn.close()
        return df
