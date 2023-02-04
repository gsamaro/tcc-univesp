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
