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

        print("\n\n*******")
        print("Dataset NaN stats")
        print(df.isna().sum())

        print("\n\n*******")
        print("Dataset Summary stats")
        print(df.dtypes)
        data_type_f = pd.DataFrame()
        for col in df.columns:
            if df.dtypes[col] == "object":
                data_type_f[col] = df[col]
        
        print("\n\n*******")
        print("Dataset to be formatted")
        print(data_type_f.head(5))

        print("\n\n*******")
        print("Unique values per columns")
        for col in data_type_f.columns:
            print(f"For column: {col}")
            print(data_type_f[col].unique())
            print(f"Entire number of row : {data_type_f.shape[0]}\nNumber of unique values: {len(data_type_f[col].unique())}")
            print("\n\n")


    
    def processing_data(self):
        pass

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(BASE_DIR, "data", "synthetic_fraud_dataset.csv")
    dataset = Processor(path=path)
    dataset.data_analysis()