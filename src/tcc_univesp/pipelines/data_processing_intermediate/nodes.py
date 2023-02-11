"""
This is a boilerplate pipeline 'data_processing_intermediate'
generated using Kedro 0.18.4
"""

import pandas as pd
from typing import List

def concat_all_enem_years(
    enem_df_year1: pd.DataFrame, 
    enem_df_year2: pd.DataFrame, 
    cols_to_select: List
    ) -> pd.DataFrame:
    return pd.concat([enem_df_year1[cols_to_select], enem_df_year2[cols_to_select]])

def get_pib(df_pib: pd.DataFrame, select_cols: list) -> pd.DataFrame:
    return df_pib[select_cols]

def get_municipios(df_cod_mun: pd.DataFrame, select_cols: list) -> pd.DataFrame:
    return df_cod_mun[select_cols]