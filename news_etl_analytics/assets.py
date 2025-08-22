from dagster import asset
from etl_script.etl import load_rrs_data_in_bronze, load_to_silver, load_to_gold,load_to_postgres


@asset
def bronze():
    load_rrs_data_in_bronze()


@asset(deps=[bronze])
def silver():
    load_to_silver()


@asset(deps=[silver])
def gold():
    load_to_gold()

@asset(deps=[gold])
def postgres():
    load_to_postgres()