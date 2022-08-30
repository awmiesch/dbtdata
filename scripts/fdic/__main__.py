"""
Map column definitions from the IDAllDefinitions_CSV subdirectory to the list of column names 
in each data file, and write the result out as yaml snippet that can be used as a dbt sources 
configuration.
"""

import sys, pathlib, pandas, re, yaml

def table_defn(name, datafile, defnfile):

    defn_table = pandas.read_table(
        defnfile,
        engine='python',
        sep=',',
        skiprows=1,
    )

    defn_lookup = {x[3]: x for x in defn_table.itertuples()}

    data_table = pandas.read_table(
        datafile,
        engine='python',
        sep=',',
    )

    columns = []
    for colname in data_table.columns:
        column = {'name': colname}

        if colname in defn_lookup.keys():
            record = defn_lookup[colname]
            column['description'] = re.sub(r'\s\s+', '', record[4].strip())
            column['meta'] = {'label': record[2]}

        columns.append(column)
    
    return {
        'name': name,
        'columns': columns,
    }


if __name__ == "__main__":
    
    version = 'All_Reports_20211231'

    if len(sys.argv) > 1:
        version = sys.argv[1]

    table_properties = {
        'debt_securities': {
            'datafile': pathlib.Path(f"fdic/{version}/{version}_Total Debt Securities.csv"),
            'defnfile': pathlib.Path("fdic/IDAllDefinitions_CSV/Total Debt Securities.csv"),
        },
        'deposits': {
            'datafile': pathlib.Path(f"fdic/{version}/{version}_Total Deposits.csv"),
            'defnfile': pathlib.Path("fdic/IDAllDefinitions_CSV/Total Deposits.csv"),
        },
        'derivatives': {
            'datafile': pathlib.Path(f"fdic/{version}/{version}_Derivatives.csv"),
            'defnfile': pathlib.Path("fdic/IDAllDefinitions_CSV/Derivatives.csv"),
        },
        'securities': {
            'datafile': pathlib.Path(f"fdic/{version}/{version}_Securities.csv"),
            'defnfile': pathlib.Path("fdic/IDAllDefinitions_CSV/Securities.csv"),
        },
    }

    tables = []
    for table_name, properties in table_properties.items():
        table = table_defn(table_name, properties['datafile'], properties['defnfile'])
        tables.append(table)
        
    sources = {
        'sources': [
            {
                'name': 'fdic',
                'database': 'dbtdata',
                'schema': 'fdic',
                'tables': tables,
            },
        ]
    }

    output_file = pathlib.Path('fdic/schema.yml')
    with open(output_file, 'w') as stream:
        yaml.dump(
            sources,
            stream,
            sort_keys=False,
        )
    print(output_file)

