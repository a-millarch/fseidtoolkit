import pandas as pd
import os
from src.utils.utils import cpr_fix
import src.utils.secret_pandas as sp

data_folder = "F:/sds_extract/FSEID-00006734/"
input_extension = ".asc"
interim_output_folder = "data/interim/cpr/chunks/"
processed_output_path = "data/processed/mapping.pkl"

encryption_key_path = 'data/dumps/cpmi.secret'

# Specify filename and CPR column name
cpr_files = {  "kontakter" : "cpr",
                    "forloeb" :"cpr",
                    "epikur" : "CPR",
                    "t_adm" : "V_CPR",
                    "t_tumor" : "K_CPRNR" 
                }

def chunk_collect_single_column(file_dict:dict):
    """ chunk through dataframe, keep only one column
    expects latin-1 encoding and ; as delimiter
    save to interim data dir"""
    for file, col_name in file_dict.items():
        print(f"chunking {file}")
        iter_df = pd.read_csv(data_folder+file+input_extension, 
                            encoding='latin-1', delimiter=";",
                            usecols = [col_name],
                            skipinitialspace=True,
                            chunksize = 100000000)
        
        for chunk in iter_df:
            chunk = chunk.drop_duplicates(subset= col_name)
            chunk[file] = True
            output_path = f"{interim_output_folder}/{file}.csv"
            chunk.to_csv(output_path, sep=";"
                        , mode='a', header=not os.path.exists(output_path))        
            
def construct_mapping_b(file_dict:dict):
    """ Expects files in interim folder"""
    dfs=[]
    for file, col_name in file_dict.items():
        df = pd.read_csv(f"{interim_output_folder}/{file}.csv", sep=";")
        df.rename(columns={col_name:"CPR"}, inplace=True)

        df["CPR"] = df["CPR"].astype(str).str.rstrip(".0")

        df = cpr_fix(df,"CPR")
        df = df.drop_duplicates(subset="CPR")
        df = df[["CPR", file]]
        dfs.append(df)
        print(f"appended {file}")

    mapping = dfs[0]
    for d in dfs[1:]:
        mapping = pd.merge(mapping, d, on="CPR", how ="outer")
    print("finished mapping")
    return mapping

def construct_mapping(file_dict: dict):
    dfs = []
    for file, col_name in file_dict.items():
        df = pd.read_csv(f"{interim_output_folder}/{file}.csv", sep=";", dtype={col_name:str})
        df.rename(columns={col_name: "CPR"}, inplace=True)

        # TODO: test if this line is obsolete now
        df["CPR"] = df["CPR"].astype(str).str.rstrip(".0")

        df = cpr_fix(df, "CPR")
        df = df.drop_duplicates(subset="CPR")
        df = df[["CPR", file]]
        dfs.append(df)
        print(f"Appended {file}. Shape: {df.shape}. Head: \n{df.head()}")

    mapping = dfs[0]
    for d in dfs[1:]:
        mapping = pd.merge(mapping, d, on="CPR", how="outer")
    print("Finished mapping. Final shape: ", mapping.shape)
    return mapping     
       
def main():

    # Collect only unique CPR's from each file
    #chunk_collect_single_column(cpr_files)
    print("finished chunking")

    # Add all CPR-files together
    mapping = construct_mapping(cpr_files)
    mapping.to_csv(f"{interim_output_folder}/mapping_interim.csv")

    # Clean up    
    mapping = mapping.fillna(False)
    
    # in some cases CPR was a float and thus the string contains .0, remove
    #mapping["CPR"] = mapping["CPR"].astype(str)
    #mapping["CPR"] = mapping["CPR"].str.rstrip(".0")
    
    mapping = mapping.drop_duplicates()
    mapping.reset_index(drop=True)

    # Add encrypted identifier
    mapping= sp.encrypt_to_new_col(mapping, 'CPR', 'PID', encryption_key_path)
    print("saving")
    # Save result
    mapping.to_pickle(processed_output_path)

if __name__ == "__main__":
    main()