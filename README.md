# dbtdata

A demo of dbt on BigQuery.

## Data Sources

The staging schema is populated from the data sources described below.

### FHLB Office of Finance Data

To generate the data files:

- Go to https://www.fhlb-of.com/ofweb_userWeb/pageBuilder/bond-data-files-74
- Download files using the links under the "Full File (Weekly)" section
- Save each file to a local directory
- Navigate to the scripts directory and run the fhlbof package
    - Provide the data file directory as the first argument
    - Provide the desired output directory as the second argument






