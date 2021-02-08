import csv
import Log.LogHeaders
from Config.ConfigProcess import ConfigProcessor

class ProcessTickerList(object):

    def __init__(self):
        config = ConfigProcessor()
        dictConfig = config.configSection('DataImport')
        self.tickerListPath = dictConfig['tickerdir']
        self.logger = Log.LogHeaders.logInitialize()

    def readCSV(self):
        with open(self.tickerListPath, 'rb') as f:
            reader = csv.reader(f)
            tickerList = list(reader)

        if len(tickerList) < 1.0 :
            self.logger.error('Failed to open the ticker list file')
            raise ValueError + "Failed to open the ticker list file"

        return tickerList

'''
if __name__ == "__main__":
    ticker = ProcessTickerList()
    lTest = ticker.readCSV()
    print lTest
    print type(lTest)
    print lTest[0][0]
'''