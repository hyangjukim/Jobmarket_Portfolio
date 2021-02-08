import os
import pandas as pd
from pymongo import MongoClient
from config import BASE_DIR
import settings

#This is intended to insert ticker lists, but can be used for other purposes

class DataFromLocal:

    def __init__(self, path=None, file_name=None):
        self.path = path
        self.file_name = file_name

    def local_data(self):

        if self.path is None:
            self.path = os.path.join(BASE_DIR, 'ticker_list')

        file_names = []
        for root, dirs, files in os.walk(self.path):
            for file in files:
                if file.endswith('.csv'):
                    file_names.append(file)

        data = []
        for file in file_names:
            data.append(pd.read_csv(os.path.join(self.path, file)))

        return data, file_names

    def connect_db(self):
        db_spec = settings.db_settings['price_time_series']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]
        #db = client['test_db']
        return db

    # build collections by file name
    def build_collections(self):
        db = self.connect_db()
        data, file_names = self.local_data()

        names = []
        for file in file_names:
            dummy = file.split('.')
            names.append(dummy[0])

        l_data = []
        for d in data:
            l_data.append(d.to_dict('records'))

        for i in range(len(l_data)):
            name = names[i]
            collection = db[name]
            pid = collection.insert_many(l_data[i]).inserted_ids

        return pid


def main():
    inst = DataFromLocal()
    inst.build_collections()

if __name__== "__main__":
    main()