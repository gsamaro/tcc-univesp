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