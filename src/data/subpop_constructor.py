import numpy as np 
import pandas as pd
import os

from src.utils.utils import cpr_fix

def construct_population_df(cpr_pickle_path:str,
                               patients_info_csv_path:str) -> pd.DataFrame:
    """HACKY ENTRYPOINT - assumes ppj cpr_df.pkl structure"""

    # read pickle with cpr's
    df = pd.read_pickle(cpr_pickle_path)
    df["CPR"] = df["ValueString"].str.replace('"', '') 
    df = df[df.ValueString != '""']
    df = cpr_fix(df, "CPR")
    df["CPR"] = pd.to_numeric(df["CPR"])
    # read target population csv
    pi = pd.read_csv(patients_info_csv_path, sep=";")
    pi.drop(columns="Unnamed: 0", inplace=True)
    # merge and return
    p = pi.merge(df[["CPR", "JournalID"]], on="JournalID")
    return p

def chunk_filter_save(population_df:pd.DataFrame, 
                    iter_csv_file_name:str,
                    filter_col:str):
    """ identify relevant contacts in LPR, save to file """

    data_folder = "F:/sds_extract/FSEID-00006734/"
    extension = ".asc"
    # TODO: Move this outisde func
    inclusion_list = population_df.CPR.unique()
    
    iter_csv = pd.read_csv(data_folder+iter_csv_file_name+extension, 
                           encoding='latin-1', delimiter=";", chunksize=100000 )
    
    df = pd.concat([chunk[chunk[filter_col].isin(inclusion_list)] for chunk in iter_csv])
    df.to_pickle("data/interim/"+iter_csv_file_name+".pkl")
    return df

def main():
    # extract relevant contacts
    population_df = construct_population_df(cpr_pickle_path = "data/interim/cpr_df.pkl", 
                                               patients_info_csv_path = "data/interim/patients_info.csv")

    file_list = ["kontakter", "forloeb"]
    for f in file_list:
        chunk_filter_save(population_df, iter_csv_file_name = f, filter_col ="cpr")
        
    chunk_filter_save(population_df, iter_csv_file_name = "epikur", filter_col ="CPR")
    chunk_filter_save(population_df, iter_csv_file_name = "t_adm", filter_col ="V_CPR")
    chunk_filter_save(population_df, iter_csv_file_name = "t_tumor", filter_col ="K_CPRNR")



if __name__ == "__main__":
    main()