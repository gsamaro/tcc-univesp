"""
This is a boilerplate pipeline 'data_processing_primary'
generated using Kedro 0.18.4
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import build_sample_and_means


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
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
    ],
    tags="pipeline_de")
