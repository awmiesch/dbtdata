# dbtdata

A demo of dbt on BigQuery.

## Data Sources

The staging schema is populated from the data sources described below.

### FHLB Office of Finance Data

1. Generate the data files:

    - Go to https://www.fhlb-of.com/ofweb_userWeb/pageBuilder/bond-data-files-74
    - Download files using the links under the "Full File (Weekly)" section
    - Save each file to a local directory
    - Navigate to the scripts directory and run the fhlbof package
        - Provide the data file directory as the first argument
        - Provide the desired output directory as the second argument

2. Create the fhlbof dataset on BigQuery:

    - Create a new project called 'dbtdata'
    - Create a new dataset called 'fhlbof'
    - Create a table for each file
        - Select 'Upload' under 'Create table from'
        - Select 'CSV' under 'File format'
        - Use the file name without the extension as the table name (eg. allbond)
        - Activate the 'Auto detect' checkbox under 'Schema'
        - Click 'Create Table' button






