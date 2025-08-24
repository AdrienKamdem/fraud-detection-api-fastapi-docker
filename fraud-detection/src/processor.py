import csv
import os
import pandas as pd
import logging
import sys


logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)

class Processor():
    
    def __init__(self, path:str):
        self.path = path
    
    def __str__(self):
        pass
    
    def data_analysis(self):
        df = pd.read_csv(self.path)
        print("Dataset NaN stats")
        print(df.isna().sum())
        print("Dataset Summary stats")
        print(df.dtypes)
    
    def processing_data(self):
        pass

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(BASE_DIR, "data", "synthetic_fraud_dataset.csv")
    dataset = Processor(path=path)
    dataset.data_analysis()