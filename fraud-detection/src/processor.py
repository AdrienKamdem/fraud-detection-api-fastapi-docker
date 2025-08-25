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
            print(type(data_type_f[col].unique()))
            print(f"Entire number of row : {data_type_f.shape[0]}\nNumber of unique values: {len(data_type_f[col].unique())}")
            print("\n\n")
        
        # All the column that have type object and that are not timestamp and that have a unique count values lower than the entire amount of data point (not primary keys) shall be mapped into categorical type data
        # ["User_ID", "Transaction_Type", "Device_Type", "Location", "Merchant_Category", "Card_Type", "Authentication_Method"]
        column_to_be_mapped = []
        
        for col in df.columns:
            if df[col].dtypes == "object" and col != "Timestamp" and len(df[col].unique()) < df.shape[0]:
                column_to_be_mapped.append(col)

        print(column_to_be_mapped)
        
        mapping_storage = {}
        
        for col in column_to_be_mapped:
            df[col], uniques = pd.factorize(df[col])
            map_ = {val: i for i, val in enumerate(uniques)}
            mapping_storage[col] = {"map_":map_}
        
        # Timestamp processing from Timestamp to Year Month Day
        
        

        
        print("Note: \nTransaction IDs are unique" \
        "\nTimestamp is mostly unique but two transaction from different user can happen at the same time." \
        "\nUser ID is unique to the user but can happen several times as a user can make several transactions")




    
    def processing_data(self):
        pass

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(BASE_DIR, "data", "synthetic_fraud_dataset.csv")
    dataset = Processor(path=path)
    dataset.data_analysis()