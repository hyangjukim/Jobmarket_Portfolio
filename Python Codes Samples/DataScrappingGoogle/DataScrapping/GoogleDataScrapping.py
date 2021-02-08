# This class stores scrapped stock data from google finance website.

import os
import csv
import urllib, datetime

from Config.ConfigProcess import ConfigProcessor

class Quote(object):

  def __init__(self):
    self.dateFormat = '%Y%m%d'
    self.timeFormat = '%H%M%S %Z'
    self.days = []
    self.symbol = ''
    self.date, self.time, self.close, self.volume = ([] for _ in range(4))

  # private function to append data to a specified dictionary
  def _insertIntoDict(self, dictKey, ticker, date, time, closing, volume, aDict):
    if not dictKey in aDict:
      aDict[dictKey] = [[ticker, date, time, closing, volume]]
    else:
      aDict[dictKey].append([ticker, date, time, closing, volume])
    return aDict

  # function that would create the dictionary which needs to be exported
  # keys will be the dates
  def genOutputDictionary(self):
    numRow = len(self.date)
    resultDict = {}
    for i in range(numRow):
      resultDict = self._insertIntoDict(self.date[i], self.symbol, self.date[i], self.time[i], self.close[i], self.volume[i], resultDict);

    return resultDict

  # exporting the dictionary into separate csv files based on their dates
  def writeCSVPerDay(self):
    output = self.genOutputDictionary()

    for i in range(len(self.days)):
      #Generate directory if not there
      outputDirByDay = self.outDirectory + self.days[i].strftime('%Y%m%d') + "/"
      if not os.path.exists(outputDirByDay):
        os.makedirs(outputDirByDay)

      dictKey = self.days[i].strftime(self.dateFormat)
      listoflistOutput = output[dictKey]
      fileName = outputDirByDay+self.symbol+self.days[i].strftime('%Y-%m-%d')+".csv"
      self.write_csv_days(fileName,listoflistOutput)

  def append(self,dt,close,volume):
    # fmt = '%Y%m%d %H:%M:%S %Z'
    d_string = dt.strftime(self.dateFormat + " " + self.timeFormat)
    date_time = d_string.split()
    #timeStamp = date_time[1].split(':')
    # print date_time[0]
    self.date.append(date_time[0])
    self.time.append(date_time[1])
    #self.time.append(timeStamp[0]+timeStamp[1]+timeStamp[2])
    self.close.append(float(close))
    self.volume.append(float(volume))

  def write_csv_days(self,filename,outputList):
    with open(filename,'w') as f:
      writer = csv.writer(f, delimiter=',',lineterminator='\n')
      writer.writerows(outputList)
      f.close()

  def write_csv(self,filename):
    with open(filename,'w') as f:
      f.write(self.to_csv())
      f.close()

class GoogleIntradayQuote(Quote):
  # intraday quotes from Google. Specify interval seconds and number of days
  def __init__(self,symbol,interval_seconds=300,num_days=5):
    super(GoogleIntradayQuote,self).__init__()
    config = ConfigProcessor()
    dictConfig = config.configSection('DataImport')
    self.url = dictConfig['url']
    self.outDirectory = dictConfig['outputdir']
    self.symbol = symbol.upper()
    url_string = self.url+self.symbol
    url_string += "&i={0}&p={1}d&f=d,c,v".format(interval_seconds,num_days)
    csv = urllib.urlopen(url_string).readlines()

    for bar in xrange(7,len(csv)):
      #print csv[bar]
      if csv[bar].count(',')!=2: continue
      offset,close,volume = csv[bar].split(',')
      if offset[0]=='a':
        day = float(offset[1:])
        self.days.append(datetime.datetime.fromtimestamp(day).date())
        offset = 0
      else:
        offset = float(offset)
      dt = datetime.datetime.fromtimestamp(day+(interval_seconds*offset))

      self.append(dt,close,volume)

if __name__ == '__main__':
  q = GoogleIntradayQuote('aapl',60,5)
  print q.writeCSVPerDay() # print it out
  print q
