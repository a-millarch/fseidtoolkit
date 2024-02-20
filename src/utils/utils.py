import numpy as np 
import pandas as pd

def cpr_tjek(df,col_name):
    return(len(df[df[col_name].astype(str).str.len() <10]))

def cpr_fix(df,col_name):
    n = cpr_tjek(df,col_name)
    print(f"\nAntal CPR (len < 10) rettet:{n}")
    
    df[col_name] = df[col_name].astype(str)
    df[col_name] = np.where(df[col_name].str.len().isin([9,]), '0'+ df[col_name], df[col_name])
    return df


def cpr_to_birthdate(input_df, cpr_col_name, new_col_name = "Birthdate"):
    #add 1900 year
    input_df[new_col_name] =  pd.to_datetime(input_df[cpr_col_name].map(lambda x: x[:4] + "19" +x[4:-4]), format="%d%m%Y", errors="coerce")
   
    # change to 2000 if rules (https://cpr.dk/media/12066/personnummeret-i-cpr.pdf)

    rules2000= [[36, 4000, 5000]
               ,[36, 9000, 10000]
               ,[57, 5000, 9000]]

    for i in rules2000:

        # fix 2000 kids

        input_df.loc[(input_df[cpr_col_name].map(lambda x: x[4:6]).astype(int) <= i[0])

                     & (input_df[cpr_col_name].map(lambda x: x[6:]).astype(int).between(i[1], i[2], inclusive='left'))

                     , new_col_name] =pd.to_datetime(input_df[cpr_col_name].map(lambda x: x[:4] + "20" +x[4:-4]), format="%d%m%Y", errors="coerce")      

    return input_df