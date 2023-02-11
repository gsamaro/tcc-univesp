"""
This is a boilerplate pipeline 'data_processing_intermediate'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    concat_all_enem_years,
    get_pib,
    get_municipios
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=concat_all_enem_years,
                inputs=[
                    "MICRODADOS_ENEM_2021",
                    "MICRODADOS_ENEM_2020",
                    "params:col_to_select"
                ],
                outputs="MICRODADOS_ENEM_2021_2020"
            ),
            node(
                func=concat_all_enem_years,
                inputs=[
                    "MICRODADOS_ENEM_2021_2020",
                    "MICRODADOS_ENEM_2019",
                    "params:col_to_select"
                ],
                outputs="MICRODADOS_ENEM_2021_2019"
            ),
            node(
                func=concat_all_enem_years,
                inputs=[
                    "MICRODADOS_ENEM_2021_2019",
                    "MICRODADOS_ENEM_2018",
                    "params:col_to_select"
                ],
                outputs="MICRODADOS_ENEM"
            ),
            node(
                func=get_pib,
                inputs=[
                    "raw_pib",
                    "params:select_cols_pib"
                ],
                outputs="int_pib",
                name="get_pib"
            ),
            node(
                func=get_municipios,
                inputs=[
                    "raw_cod_municipios",
                    "params:select_cols_municipios"
                ],
                outputs="int_municipios",
                name="get_municipios"
            ),
    ],
    tags="pipeline_de")