# dbtdata

A demo of a dbt project that uses BigQuery as the backend.

## Source Data

Follow the steps below to create the BigQuery source datasets. Source datasets represent tables from on-prem databases that have been uploaded to BigQuery via CSV files.

### FHLB Office of Finance (fhlbof)

1. Create a new dataset called *fhlbof*
2. Change the *database* entry of *models/staging/fhlbof/schema.yml* to the project name where the dataset was created
3. Create a new table for each CSV file in the *sourcedata/fhlbof* subdirectory
    - Select *Create table* from the *fhlbof* dataset
        - Select *Upload* under *Create table from*
        - Select *CSV* under *File format*
        - Use the file name without the extension as the table name (eg. bond)
        - Enable the *Auto detect* option
        - Click *Create Table*

#### To generate the data files (optional):

- Go to https://www.fhlb-of.com/ofweb_userWeb/pageBuilder/bond-data-files-74
- Download files using the links under the *Daily Issuance Files* section
- Select a day and save each file to a local directory
- Navigate to the scripts directory and run the fhlbof package
    - Provide the local data file directory as the first argument
    - Provide the desired local output directory as the second argument
    - Provide the effective date of the data files as the third argument (using YYYY-MM-DD format)






