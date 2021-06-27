import pandas as pd
import json

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

    def loadData(self, _df, table_name, conn):

        _df.to_sql(name = table_name, schema = 'abars', con = conn, if_exists='append', index = False)
        conn.commit()
