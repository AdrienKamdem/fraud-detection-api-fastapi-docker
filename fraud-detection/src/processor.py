import csv
import os
import pandas as pd
import logging
import matplotlib.pyplot as plt


logger = logging.getLogger()
logger.setLevel(level=logging.DEBUG)

class Processor():
    
    def __init__(self, path:str):
        self.path = path
    
    def __str__(self):
        pass
    
    def processing_data(self)->pd.DataFrame:

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

        print("\n\nColumns to be mapped")
        print(column_to_be_mapped)
        
        mapping_storage = {}
        
        for col in column_to_be_mapped:
            df[col], uniques = pd.factorize(df[col])
            map_ = {val: i for i, val in enumerate(uniques)}
            mapping_storage[col] = {"map_":map_}

        print("\n\nExample")
        print(df[column_to_be_mapped[0]])
        
        # Timestamp processing from Timestamp to Year Month Day
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
        df["year"]   = df["Timestamp"].dt.year
        df["month"]  = df["Timestamp"].dt.month
        df["day"]    = df["Timestamp"].dt.day
        df["hour"]   = df["Timestamp"].dt.hour
        df["minute"] = df["Timestamp"].dt.minute
        df["second"] = df["Timestamp"].dt.second

        print("\n\nDate Processing Results")
        print(df.columns)
        print(df.head(5))

        # Processing Done
        print(df.dtypes)
        
        print("Note: \nTransaction IDs are unique" \
        "\nTimestamp is mostly unique but two transaction from different user can happen at the same time." \
        "\nUser ID is unique to the user but can happen several times as a user can make several transactions")

        return df
    
    def data_analysis(self, df:pd.DataFrame):
        col_stats_ = []
        for col in df.columns:
            if df[col].dtypes == "float64":
                col_stats_.append(col)
        print(df[col_stats_].describe())
        print("\n\nFinal analysis")
        print(df[["IP_Address_Flag", "Previous_Fraudulent_Activity", "Daily_Transaction_Count", "Failed_Transaction_Count_7d", "Card_Age", "Is_Weekend"]].head(5))

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(BASE_DIR, "data", "synthetic_fraud_dataset.csv")
    dataset = Processor(path=path)
    df = dataset.processing_data()
    dataset.data_analysis(df=df)