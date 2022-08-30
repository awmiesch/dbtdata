"""
Convert each .dat file into a .csv file.
"""

import sys, argparse, re, pandas, yaml
from pathlib import Path


def clean(value):
    return re.sub(r'[%]+', '', value.strip())


def split_row(row, ranges):
    values = []
    for r in ranges:
        value = row[r[0]:r[1]]
        value = clean(value)
        values.append(value)
    return values


def transform(column, metadata):
    datefn = lambda x: pandas.to_datetime(x, format=metadata['datefmt'], errors='coerce')
    numfn = lambda x: pandas.to_numeric(x, errors='coerce')
    strfn = lambda x: x
    dtmap = {
        'datetime64': datefn,
        'float64': numfn,
        'int64': numfn,
        'object': strfn,
    }
    fn = dtmap.get(metadata['dtype'], strfn)
    return fn(column)


def main(datadir: Path, outputdir: Path, sourcedir: Path):

    for properties_file in sourcedir.glob("*.yml"):
        
        table_name = properties_file.stem

        with open(properties_file, 'r') as stream:
            properties = yaml.load(stream, Loader=yaml.SafeLoader)
        
        datafile = datadir.joinpath(f"{table_name}.dat")
    
        with open(datafile, 'r') as stream:
            rows = stream.readlines()
        
        header_row = properties['header_row']
        start_row = properties['skip_rows']
        field_ranges = [x['range'] for x in properties['columns']]

        colnames = split_row(rows[header_row], field_ranges)

        records = []
        for row in rows[start_row:]:
            values = split_row(row, field_ranges)
            record = dict(zip(colnames, values))
            records.append(record)
        
        table = pandas.DataFrame(
            columns=colnames,
            data=records,
        )

        column_metadata = {x['name']: x for x in properties['columns']}

        transformed_columns = {}
        for colname, column in table.iteritems():
            metadata = column_metadata[colname]
            transformed_columns[colname] = transform(column, metadata)
        
        table = pandas.DataFrame(transformed_columns)

        output_file = outputdir.joinpath(f"{table_name}.csv")
        table.to_csv(
            output_file,
            index=False,
            na_rep='',
        )

        print(output_file)


def main1(properties_file):
    with open(properties_file, 'r') as stream:
        properties = yaml.load(stream, Loader=yaml.SafeLoader)

    with open(properties['data_file'], 'r') as stream:
        rows = stream.readlines()

    field_ranges = [x['range'] for x in properties['columns']]
    
    colnames = split_row(rows[properties['header_row']], field_ranges)

    records = []
    for row in rows[properties['skip_rows']:]:
        values = split_row(row, field_ranges)
        record = dict(zip(colnames, values))
        records.append(record)
    
    table = pandas.DataFrame(
        columns=colnames,
        data=records,
    )

    column_metadata = {x['name']: x for x in properties['columns']}

    transformed_columns = {}
    for colname, column in table.iteritems():
        metadata = column_metadata[colname]
        transformed_columns[colname] = transform(column, metadata)
    
    table = pandas.DataFrame(transformed_columns)

    output_file = properties_file.parent.joinpath(f"{properties_file.stem}.csv")

    table.to_csv(
        output_file,
        index=False,
        na_rep='',
    )

    print(output_file)


if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser(description="Convert OF DAT files to CSV files.")
    argparser.add_argument(
        "datadir",
        type=Path,
        help="Path to data file directory.",
    )
    argparser.add_argument(
        "--outputdir",
        type=Path,
        help="Path to output file directory. Defaults to user home directory.",
        default=Path.home(),
    )
    argparser.add_argument(
        "--sourcedir",
        type=Path,
        help="Path to directory containing yaml properties file for each table. Defaults to fhlbof/tabledefs.",
        default=Path(__file__).parent.joinpath('tabledefs'),
    )

    args = argparser.parse_args()
    
    main(args.datadir, args.outputdir, args.sourcedir)

