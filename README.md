# dbtdata

A demo of a dbt project that uses BigQuery as the backend.

## Source Data

Follow the steps below to create the BigQuery source datasets. Source datasets represent tables from on-prem databases that have been uploaded to BigQuery via CSV files.

### FHLB Office of Finance (fhlbof)

1. Download files https://www.fhlb-of.com/ofweb_userWeb/pageBuilder/bond-data-files-74 (under daily)
2. Create a new dataset called *fhlbof*
3. Change the *database* entry of *models/staging/fhlbof/schema.yml* to the project name where the dataset was created

#### To generate the data files (optional):

- Go to https://www.fhlb-of.com/ofweb_userWeb/pageBuilder/bond-data-files-74







