"""
This is a boilerplate pipeline 'data_processing_primary'
generated using Kedro 0.18.4
"""

import pandas as pd
from typing import List
import numpy as np

def _normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    df["NU_ANO"] = df["NU_ANO"].astype(int)
    df["TP_ESCOLA"] = df["TP_ESCOLA"].astype(int)
    df["NU_NOTA_MT"] = df["NU_NOTA_MT"].astype(float)
    df["TP_PRESENCA_MT"] = df["TP_PRESENCA_MT"].astype(float)
    return df

def _filter_present(df: pd.DataFrame) -> pd.DataFrame:
    return df.query("TP_PRESENCA_MT == 1").copy()

def _make_sample(df: pd.DataFrame, number_of_sample: int, sample_size: int, year: int):    
    sample_publica = np.random.choice(
        df.query(
            f"NU_ANO == {year} and TP_ESCOLA == 2")["NU_NOTA_MT"], 
            size=(number_of_sample, sample_size)
        )
    sample_privada = np.random.choice(
        df.query(
            f"NU_ANO == {year} and TP_ESCOLA == 3")["NU_NOTA_MT"], 
            size=(number_of_sample, sample_size)
        )
    return sample_publica, sample_privada

def _build_average(sample_publica: np.array, sample_privada: np.array):
    mean_sample_publica = sample_publica.mean(axis=1)
    mean_sample_privada = sample_privada.mean(axis=1)
    return mean_sample_publica, mean_sample_privada

def build_sample_and_means(df: pd.DataFrame, number_of_sample: int, sample_size: int, year: int):
    df = _normalize_dtypes(df)
    df = _filter_present(df)
    sample_publica, sample_privada = _make_sample(df, number_of_sample, sample_size, year)
    mean_sample_publica, mean_sample_privada = _build_average(sample_publica, sample_privada)
    return pd.DataFrame(
        np.array(
            [
                mean_sample_publica, 
                mean_sample_privada
            ]).reshape(2, -1).T,
        columns=[f"mean_{year}_publica", f"mean_{year}_privado"]
    )

def assemble_samples(df: pd.DataFrame, new_df: pd.DataFrame):
    return pd.concat([df, new_df], axis=1)

def _adjust_df(df: pd.DataFrame) -> pd.DataFrame:
    df1 = df[[df.columns[0]]].copy()
    col_name_splited = str(df1.columns[0]).split("_")
    year = col_name_splited[1]
    school_flag = col_name_splited[2]
    df1["ano"] = year
    df1["tp_escola"] = school_flag
    df1.rename(columns={str(df1.columns[0]): "mean"}, inplace=True)

    df2 = df[[df.columns[1]]].copy()
    col_name_splited = str(df2.columns[0]).split("_")
    year = col_name_splited[1]
    school_flag = col_name_splited[2]
    df2["ano"] = year
    df2["tp_escola"] = school_flag
    df2.rename(columns={str(df2.columns[0]): "mean"}, inplace=True)

    return pd.concat([df1, df2])

def build_adjusted_df(df_2021: pd.DataFrame, df_2020: pd.DataFrame, df_2019: pd.DataFrame, df_2018: pd.DataFrame) -> pd.DataFrame:
    df_2021_adjusted = _adjust_df(df_2021)
    df_2020_adjusted = _adjust_df(df_2020)
    df_2019_adjusted = _adjust_df(df_2019)
    df_2018_adjusted = _adjust_df(df_2018)
    return pd.concat([df_2021_adjusted, df_2020_adjusted, df_2019_adjusted, df_2018_adjusted], ignore_index=True)

def join_pib_municipios(
    df_pib: pd.DataFrame, 
    df_municipios: pd.DataFrame,
    parameters
    ) -> pd.DataFrame:
    return pd.merge(df_pib, df_municipios, left_on=parameters["left_on_pib"], right_on=parameters["right_on_municipios"])
