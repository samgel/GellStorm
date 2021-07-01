import sys
sys.path.insert(0,'/workplace/gellstorm/sqlite')
sys.path.insert(0,'/workplace/gellstorm/alpaca')

import pandas as pd
import json
import sqliteUtility

class DataLoadClient():
    def __init__(self):
        pass

    def jsonClean(self,json_data):
        output = []
        for key, value in json_data.items():
            for bar in value['bars']:
                temp_dict = {}
                temp_dict['ticker'] = key
                for inner_key, inner_value in bar.items():
                    temp_dict[inner_key] = inner_value
                output.append(temp_dict)
        output_df = pd.DataFrame.from_dict(pd.json_normalize(output), orient='columns')
        return output_df

    def loadInsertData(self, df, table_name,  conn):
        self.sqliteUtility = sqliteUtility.sqliteUtility()
        self.columns = self.sqliteUtility.get_columns(conn, table_name)
        self.df = df
        self.df.columns = self.columns
        self.df.to_sql(name = table_name, schema = 'GellstormDB', con = conn, if_exists='append', index = False)
        conn.commit()
