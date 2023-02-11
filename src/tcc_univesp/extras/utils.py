import pandas as pd
from typing import Dict

def join(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    parameters: Dict
) -> pd.DataFrame:
    df_left = df_left.convert_dtypes()
    df_right = df_right.convert_dtypes()
    return pd.merge(
        df_left,
        df_right,
        left_on=parameters["left_on"],
        right_on=parameters["right_on"],
        how=parameters["how"]
    )