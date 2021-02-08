'''
Created on Apr 16, 2015

@author: hyakim
'''

import logging

def logInitialize():    
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    hdlr = logging.FileHandler('../Log/myapp.log')
    logger.addHandler(hdlr)
    return logger
