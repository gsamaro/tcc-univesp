from kedro.io import AbstractDataSet
import numpy as np
import basedosdados as bd
from typing import Dict, Any
import pandas as pd

class BdDataset(AbstractDataSet[np.ndarray, np.ndarray]):
    
    def __init__(self, filepath: str):
        """Creates a new instance of ImageDataSet to load / save image data at the given filepath.

        Args:
            filepath: The location of the image file to load / save data.
        """
        self._filepath = filepath

    def _load(self) -> pd.DataFrame:
        """Loads data from the image file.

        Returns:
            Data from the image file as a numpy array.
        """
        return bd.read_table(
            dataset_id='br_ibge_pib',
            table_id='municipio',
            billing_project_id="tcc-univesp-374315"
        )

    def _save(self, data: np.ndarray) -> None:
        """Saves image data to the specified filepath"""
        return None

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset"""
        pass