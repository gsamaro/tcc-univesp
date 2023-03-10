"""
This is a boilerplate pipeline 'data_processing_primary'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    build_sample_and_means,
    assemble_samples,
    build_adjusted_df,
    join_pib_municipios,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
    [
        node(
                func=build_sample_and_means,
                inputs=[
                    "MICRODADOS_ENEM",                    
                    "params:number_of_sample",
                    "params:sample_size",
                    "params:year_2021"
                ],
                outputs="sample_MICRODADOS_ENEM_2021"
            ),
        node(
                func=build_sample_and_means,
                inputs=[
                    "MICRODADOS_ENEM",                    
                    "params:number_of_sample",
                    "params:sample_size",
                    "params:year_2020"
                ],
                outputs="sample_MICRODADOS_ENEM_2020"
            ),
        node(
                func=build_sample_and_means,
                inputs=[
                    "MICRODADOS_ENEM",                    
                    "params:number_of_sample",
                    "params:sample_size",
                    "params:year_2019"
                ],
                outputs="sample_MICRODADOS_ENEM_2019"
            ),
        node(
                func=build_sample_and_means,
                inputs=[
                    "MICRODADOS_ENEM",                    
                    "params:number_of_sample",
                    "params:sample_size",
                    "params:year_2018"
                ],
                outputs="sample_MICRODADOS_ENEM_2018"
            ),
        node(
                func=assemble_samples,
                inputs=[
                    "sample_MICRODADOS_ENEM_2018",
                    "sample_MICRODADOS_ENEM_2019"
                ],
                outputs="sample_2018_2019"
        ),
        node(
                func=assemble_samples,
                inputs=[
                    "sample_2018_2019",
                    "sample_MICRODADOS_ENEM_2020"
                ],
                outputs="sample_2018_2019_2020"
        ),
        node(
                func=assemble_samples,
                inputs=[
                    "sample_2018_2019_2020",
                    "sample_MICRODADOS_ENEM_2021"
                ],
                outputs="sample_all_years"
        ),
        node(
            func=build_adjusted_df,
            inputs=[
                "sample_MICRODADOS_ENEM_2021",
                "sample_MICRODADOS_ENEM_2020",
                "sample_MICRODADOS_ENEM_2019",
                "sample_MICRODADOS_ENEM_2018"
            ],
            outputs="sample_all_year_adjusted"
        ),
        node(
            func=join_pib_municipios,
            inputs=[
                "int_pib",
                "int_municipios",
                "params:join_pib_municipios",
            ],
            outputs="pri_pib",
            name="join_pib_municipios"
        ),
    ],
    tags="pipeline_de")
