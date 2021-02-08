'''
This class processes the configuration file and passes information to the main module.

example:

config = ConfigProcessor()
dictConfig = config.configSection('DataImport')

'''

import ConfigParser 
import Log.LogHeaders

class ConfigProcessor(object):

    def __init__(self, configPath = '../Config/configuration.cfg'):
        self.configPath = configPath
        self._parser = ConfigParser.SafeConfigParser()
        self.parsedFile = self._parser.read(self.configPath)
        self.sectionNames = self._parser.sections()
        self.logger = Log.LogHeaders.logInitialize()

        if len(self.parsedFile) < 1.0 :    
            self.logger.error('Failed to open the configuration file')        
            raise ValueError, "Failed to open the configuration file"

    # pass the section name that needs to be imported
    def configSection(self, section_name): 
        dictConfig = {}        
        for name, value in self._parser.items(section_name):
            dictConfig[name] = value
        
        return dictConfig

if __name__ == "__main__":
    config = ConfigProcessor()
    dTest = config.configSection('DataImport')    
    print config.sectionNames
    print dTest
