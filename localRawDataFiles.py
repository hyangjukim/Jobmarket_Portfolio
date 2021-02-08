# read in the RAW data from csv files and return the object format
# This will be deleted or

import os
import pandas as pd
from config import LOCAL_DATA

# this class will mostly be used for initial development purpose
class LocalRawDataProcessing:

    def __init__(self, path = ''):
        if path == "":
            self.path = LOCAL_DATA
        else:
            self.path = path

        self.formattedData = self.batchFormattedData()

    # import "one" raw single name
    def importData(self, file_name):
        path = os.path.join(self.path, file_name)
        data = pd.read_csv(path)
        #data = data.set_index('Date')


        '''
        raw_column_names = list(data.columns.values)
        column_names =[]
        for name in raw_column_names:
            if name != 'Date':
                column_names.append(file_name + ' ' + name)
            else:
                column_names.append(name)

        data.columns = column_names'''

        return data

    # Generates the base data format - dictionary of the single name
    def processData(self, file_path):

        pdData = self.importData(file_path)
        instance = formatDataInstances()
        dData = instance.addTimeSeries(pdData)

        return dData

    # Returns batch of single names
    def batchFormattedData(self):
        return 1

def main():
    inst = LocalRawDataProcessing()
    data = inst.importData('AAPL.csv')
    print(data)

if __name__ ==  "__main__":
    main()


