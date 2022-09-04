"""
Convert each .dat file into a .csv file.
https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
"""

import argparse, re, yaml, datetime
from os import getenv
from pathlib import Path
from pandas import DataFrame, Series, to_datetime, to_numeric
from google.cloud import bigquery as bq


def clean(value: str) -> str:
    return re.sub(r'[%]+', '', value.strip())


def split_row(row: tuple, ranges: list) -> list:
    values = []
    for r in ranges:
        value = row[r[0]:r[1]]
        value = clean(value)
        values.append(value)
    return values


def transform(column: Series, metadata: dict) -> Series:
    datefn = lambda x: to_datetime(x, format=metadata['datefmt'], errors='coerce')
    numfn = lambda x: to_numeric(x, errors='coerce')
    strfn = lambda x: x
    dtmap = {
        'datetime64[ns]': datefn,
        'float64': numfn,
        'int64': numfn,
        'object': strfn,
    }
    fn = dtmap.get(metadata['dtype'], strfn)
    return fn(column)


def get_colnames(rows: list, properties: dict) -> list:
    header_row = properties['header_row']
    field_ranges = [x['range'] for x in properties['columns']]
    return split_row(rows[header_row], field_ranges)


def read_data_file(path: Path, properties: dict) -> DataFrame:
    
    with open(path, 'r') as stream:
        rows = stream.readlines()   

    field_ranges = [x['range'] for x in properties['columns']]
    start_row = properties['skip_rows']
    colnames = get_colnames(rows, properties)

    records = []
    for row in rows[start_row:]:
        values = split_row(row, field_ranges)
        record = dict(zip(colnames, values))
        records.append(record) 
        
    table = DataFrame(
        columns=colnames,
        data=records,
    ) 

    column_metadata = {x['name']: x for x in properties['columns']}

    transformed_columns = {}
    for colname, column in table.iteritems():
        metadata = column_metadata[colname]
        transformed_columns[colname] = transform(column, metadata)
    
    table = DataFrame(transformed_columns)

    return table


def create_schema(table: DataFrame) -> list:
    dtmap = {
        'object': 'STRING',
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'datetime64[ns]': 'DATE',
    }
    schema = []
    for name, dtype in zip(table.columns, table.dtypes):
        field = bq.SchemaField(name, dtmap.get(dtype.name, "STRING"))
        schema.append(field)   
    return schema


def read_properties_file(path: Path) -> dict:
    with open(path, 'r') as stream:
        properties = yaml.load(stream, Loader=yaml.SafeLoader)

    return properties


def load_tables(client: bq.Client, datadir: Path, dataset: str):

    sourcedir = Path(__file__).parent.joinpath('tabledefs')

    for properties_file in sourcedir.glob("*.yml"):     

        table_name = properties_file.stem   
        table_id = f"{getenv('GCP_PROJECT')}.{dataset}.{table_name}"
        table_ref = bq.table.TableReference.from_string(table_id)
        print(table_name)

        properties = read_properties_file(properties_file)
        
        # Ignore the first 3 characters of the filename.
        data_file = [x for x in datadir.glob(f"*{table_name}.dat")][0]

        df = read_data_file(data_file, properties)
        df['load_timestamp'] = datetime.datetime.now()
        print(df.head())

        job = client.load_table_from_dataframe(
            df,
            table_ref,
            project=getenv('GCP_PROJECT'),
        )

        jobrun = job.result()

        print(f"job_id: {jobrun.job_id}")
        print(f"job_status: {jobrun.state}")

        table = client.get_table(table_id)

        print(f"table: {table.full_table_id}")
        print(f"row_count: {table.num_rows}")


def drop_tables(client: bq.Client, dataset: str):
    for table in client.list_tables(dataset):
        client.delete_table(table)
        print(f"Dropped table {table.full_table_id}")


def main(datadir: Path, dataset: str):

    client = bq.Client()

    drop_tables(client, dataset)

    load_tables(client, datadir, dataset)


if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser(description="Upload DAT files to BigQuery.")
    argparser.add_argument(
        "datadir",
        type=Path,
        help="Path to data file directory.",
    )
    argparser.add_argument(
        "dataset",
        type=str,
        help="Dataset name.",
    )

    args = argparser.parse_args()
    
    main(args.datadir, args.dataset)

