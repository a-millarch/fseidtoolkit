import pandas as pd
import os
import yaml 

from src.utils.utils import cpr_fix
import src.utils.secret_pandas as sp

with open('conf/defaults.yaml') as file:
    cfg = yaml.safe_load(file)
    
cpr_files = cfg.get('cpr_files', {})

def chunk_collect_single_column(file_dict:dict):
    """
    Chunk through a dataframe, keeping only one specified column.

    Args:
        file_dict (dict): A dictionary containing file names as keys and column names as values.

    Notes:
        expects data files to be encoded in Latin-1 and delimited by ';'.

    Saves:
        dfs with single column to the interim data directory.

    Example:
        >>> file_dict = {'data/raw/data_file.csv': 'CPR'}
        >>> chunk_collect_single_column(file_dict)"""
    
    for file, col_name in file_dict.items():
        print(f"chunking {file}")
        iter_df = pd.read_csv(cfg["data_folder"]+file+cfg["input_extension"], 
                            encoding='latin-1', delimiter=";",
                            usecols = [col_name],
                            skipinitialspace=True,
                            chunksize = 100000000)
        
        for chunk in iter_df:
            chunk = chunk.drop_duplicates(subset= col_name)
            chunk[file] = True
            output_path = f"{cfg['interim_output_folder']}/{file}.csv"
            chunk.to_csv(output_path, sep=";"
                        , mode='a', header=not os.path.exists(output_path))        

def construct_mapping(file_dict: dict)-> pd.DataFrame:
    """
    Constructs a mapping DataFrame based on data from files specified in `file_dict`.
    
    Args:
        file_dict (dict): A dictionary where keys are file names and values are column names.

    Notes:
        This function assumes that the data files are CSVs and 
        reside in the interim output folder specified in the config (`cfg`).
        Each file is expected to have a column with unique identifiers (i.e., CPR numbers).
    """
    dfs = []
    for file, col_name in file_dict.items():
        df = pd.read_csv(f"{cfg['interim_output_folder']}/{file}.csv", sep=";", dtype={col_name:str})
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
    chunk_collect_single_column(cpr_files)
    print("finished chunking")

    # Add all CPR-files together
    mapping = construct_mapping(cpr_files)
    mapping.to_csv(f"{cfg['interim_output_folder']}/mapping_interim.csv")

    # Clean up    
    mapping = mapping.fillna(False)  
    mapping = mapping.drop_duplicates()
    mapping.reset_index(drop=True)

    # Add encrypted identifier
    mapping= sp.encrypt_to_new_col(mapping, 'CPR', 'PID', cfg['encryption_key_path'])
    print("saving")

    # Save result
    mapping.to_pickle(cfg["processed_output_path"])

if __name__ == "__main__":
    main()