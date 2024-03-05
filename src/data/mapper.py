import pandas as pd
import numpy as np
from pathlib import Path
import os
import yaml 
import logging
from src.common.log_config import setup_logging, clear_log
from src.utils.utils import cpr_fix

setup_logging()
logger = logging.getLogger(__name__)

with open('conf/defaults.yaml') as file:
    cfg = yaml.safe_load(file)
    
cpr_files = cfg.get('cpr_files', {})

def replace_cpr(file_dict:dict, mapping:pd.DataFrame):
    """ 
    Args:    
        - file_dict (dict): A dictionary containing file names as keys and column names as values.
        - mapping (pd.DataFrame): DataFrame containing CPR to PID mapping.

    Saves:
        - None

    Notes:
        - chunk through dataframe, 
            expects latin-1 encoding and ; as delimiter
        - add mapping column,
            new col "PID"
        - remove CPR columns,
        - save to disk.

    requires config object
    """
    n_files=0
    for file, col_name in file_dict.items():
        n_files=n_files+1
        logger.info(f"{n_files}/{len(file_dict.keys())} - chunking {file}")
    
        iter_df = pd.read_csv(cfg["data_folder"]+file+cfg["input_extension"], 
                            encoding='latin-1', delimiter=";",
                            dtype={col_name:str},
                            skipinitialspace=True,
                            chunksize = cfg["chunksize"])


        # reduce mapping for each loop
        tmp_map = mapping[mapping[file] == True]
        tmp_map = tmp_map[["CPR", "PID"]]
        logger.debug(f"map shape: {tmp_map.shape}\n{tmp_map.head(3)}")
        
        for chunk in iter_df:
             # some raw files had CPR as float input. strip .0 and fix leading 0 if missing
            chunk[col_name] = chunk[col_name].str.rstrip(".0")
            chunk = cpr_fix(chunk, col_name)
            chunk = chunk.merge(tmp_map, left_on=col_name, right_on="CPR", how='left')
            chunk = chunk.drop(columns=[col_name, "CPR"])
            logger.debug(f"start len: {len(chunk)}")
            # append save
            if cfg["format"] == "csv":
                output_path = f"{cfg['mapped_output_folder']}/{file}.csv"
                chunk.to_csv(output_path, sep=";"
                        , mode='a', header=not os.path.exists(output_path))
                  
            elif cfg["format"] == "parquet":    
                chunk = chunk.astype(str)
                chunk.fillna("NaN", inplace=True)
                output_path = Path(f"{cfg['mapped_output_folder']}/{file}.parquet")
                if output_path.exists():
                    chunk.to_parquet(output_path,
                                    engine = "fastparquet",
                                    compression = "gzip",
                                    append=True, 
                                    index = False,
                                    )  
                else:
                    chunk.to_parquet(output_path,
                                    engine = "fastparquet",
                                    compression = "gzip",
                                    append=False, 
                                    index = False,
                                    )  
            else:
                logger.error("Unimplemented format for output data")

            logger.info("chunk finished")
            logger.debug(f"new len:{len(chunk)}")
            
def main():
    mapping = pd.read_pickle(cfg["processed_output_path"])
    replace_cpr(cpr_files, mapping)

if __name__=="__main__":
    main()

