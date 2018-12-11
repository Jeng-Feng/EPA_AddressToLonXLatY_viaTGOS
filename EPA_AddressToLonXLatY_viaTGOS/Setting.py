
import configparser # https://docs.python.org/3/library/configparser.html#supported-ini-file-structure

class Config():

    @staticmethod   # python @classmethod and @ staticmethod �����P(���O), http://missions5.blogspot.tw/2014/12/python-classmethod-and-staticmethod.html
    def Value(Section,Key):
        config =  configparser.ConfigParser()
        config.read('AppConfig.ini')        
        return config[Section][Key]
        
        

