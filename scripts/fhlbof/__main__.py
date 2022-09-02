"""
Convert each .dat file into a .csv file.
https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.load_table_from_file
"""

import argparse, re, datetime, yaml
from os import getenv
from pathlib import Path
from pandas import DataFrame, Series, to_datetime, to_numeric
from google.cloud import bigquery


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


def read_file(datafile: Path, properties: dict, effective_date: str) -> DataFrame:
    
    with open(datafile, 'r') as stream:
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
    table['EFFECTIVE_DATE'] = datetime.date.fromisoformat(effective_date)

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
        field = bigquery.SchemaField(name, dtmap.get(dtype.name, "STRING"))
        schema.append(field)   
    return schema


def main(datadir: Path, effective_date: str, sourcedir: Path):

    for properties_file in sourcedir.glob("*.yml"):     

        table_name = properties_file.stem   
        print(table_name)

        with open(properties_file, 'r') as stream:
            properties = yaml.load(stream, Loader=yaml.SafeLoader)
        
        # Ignore the first 3 characters of the filename.
        datafile = [x for x in datadir.glob(f"*{table_name}.dat")][0]
        
        df = read_file(datafile, properties, effective_date)

        print(df.head())

        #schema = create_schema(df)

        table_id = f"{getenv('GCP_PROJECT')}.fhlbof.{table_name}"
        table_ref = bigquery.table.TableReference.from_string(table_id)
        
        client = bigquery.Client()

        job = client.load_table_from_dataframe(
            df,
            table_ref,
            project=getenv('GCP_PROJECT'),
        )

        r = job.result()

        print(type(r))

        table = client.get_table(table_id)

        print(f"table: {table.full_table_id}\nrow_count:{table.num_rows}")


if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser(description="Upload DAT files to BigQuery.")
    argparser.add_argument(
        "datadir",
        type=Path,
        help="Path to data file directory.",
    )
    argparser.add_argument(
        "effective_date",
        type=str,
        help="Effective date in YYYY-MM-DD format",
    )
    argparser.add_argument(
        "--sourcedir",
        type=Path,
        help="Path to directory containing yaml properties file for each table. Defaults to fhlbof/tabledefs.",
        default=Path(__file__).parent.joinpath('tabledefs'),
    )

    args = argparser.parse_args()
    
    main(args.datadir, args.effective_date, args.sourcedir)

