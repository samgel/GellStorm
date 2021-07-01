import sqlite3
import pandas as pd
from sqlite3 import Error



class sqliteUtility():
    def __init__(self):
        pass

    def create_connection(self, db_file):
        """ create a database connection to a SQLite database """
        self.conn = sqlite3.connect(db_file)
        return self.conn


    def execute_statement(self, conn, sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        self.c = conn.cursor()
        self.c.execute(sql)

    def execute_query(self, conn, sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        #self.c = conn.cursor()
        #self.c.execute(sql)
        #self.rows = self.c.fetchall()
        self.rows = pd.read_sql(sql, conn)
        return self.rows

    def get_columns(self, conn, table):
        self.c = conn.cursor()
        self.c.execute("select * from {}".format(table))
        self.columns = [description[0] for description in self.c.description]
        return self.columns

if __name__ == '__main__':
    pass