import logging
import os
from datetime import datetime

class customLog:
    def __init__(self, file_name):
        self.file_name = file_name
        self.path = f"{os.getcwd()}/logs/"
        # DateTime:Level:Arquivo:Mensagem
        log_format = '%(asctime)s:%(levelname)s:%(filename)s:%(message)s'
        logging.basicConfig(filename=f"{self.path + self.file_name}_{datetime.today().strftime('%Y-%m-%d')}.log",
                    # w -> sobrescreve o arquivo a cada log
                    # a -> não sobrescreve o arquivo
                    filemode='w',
                    level=logging.DEBUG,
                    format=log_format)
        self.logger = logging.getLogger('root')
    def setLog(self, messege, print_terminal=False):
        self.logger.info(messege)
        if print_terminal:
            print(messege)
    def setError(self, messege, print_terminal=False):
        self.logger.error(messege)
        if print_terminal:
            print(messege)
