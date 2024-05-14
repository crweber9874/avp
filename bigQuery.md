# Voter Project Documentation
The combined data is stored <a href="[your_url_here](https://www.dropbox.com/scl/fo/t1ivhinoc2byyo6v2vcm4/h?rlkey=c7j0qs6l19ceguhlyppgmppwv&dl=0)">here</a>. Every time I receive data, I drop it to this folder. Prior to uploading it to this folder, I "clean" the data to ensure consistent headers.


The only dataset that is relevant to the analysis presented thus far is **AVPv2**

The data are then "cleaned" with the following python code. This file is saved as **clean_headers.py**.


```{python}

# Geo Preprocessing
import pandas as pd
import numpy as np
import os
import glob
import re
import datetime
from datetime import date

path = r"/Users/Chris/Dropbox/batch_upload/"
os.chdir(path)
csv_files = glob.glob(os.path.join(path, "*.csv"))

regInfo = ['Registrant ID', 'Registration Date',
           'Effective Date of Change',
           'Year of Birth',
           'LastName',
           'FirstName',
           'MiddleName', 'Suffix', 'Status', 'Status Reason', 'County',
           'HouseNumber', 'StreetPrefix', 'StreetName', 'StreetType',
           'StreetSuffix', 'UnitType', 'UnitNumber', 'Residence Address',
           'Residence City', 'Residence State', 'Residence Zip',
           'MailingAddress',
           'MailingAddress2', 'MailingCity', 'MailingState', 'MailingZip',
           'MailingCountry', 'Party', 'PEVL', 'Phone', 'Occupation',
           'PrecinctPart', 'Congressional', 'Board of Supervisors', 'Legislative',
           'Municipal', 'School', 'Fire', 'Precinct']

electionList = ['03/22/2016-2016 PRES PREF', '08/30/2016-2016 PRIMARY ELECTION',
                '11/08/2016-2016 GENERAL ELECTION', '08/28/2018-2018 PRIMARY ELECTION',
                '11/06/2018-2018 GENERAL ELECTION', '03/17/2020-2020 Presidential Preference', '08/04/2020-PRIMARY 2020',
                '11/03/2020-GENERAL 2020', '08/02/2022-2022 PRIMARY ELECTION',
                '08/02/2022-PRIMARY 2022', '11/08/2022-GENERAL 2022']



def check_header(df, electionList, regInfo):
    combined = electionList + regInfo
    columnList = regInfo + electionList
    columns_present = list(set(df.columns).intersection(columnList))
    df = df[columns_present]
    for election in combined:
        if election not in df.columns:
            df[election] = np.nan
        else:
            df = df
    valid_columns = []
    for column in df.columns:
        column = re.sub(r'[^a-zA-Z0-9_]', '_', column)
        column = re.sub(r'^\d+', '_', column)
        valid_columns.append(column)
    df.columns = valid_columns
    sorted_columns = sorted(df.columns)
    return df[sorted_columns]

for f in csv_files:
   path_name = os.path.split(f)[-1]
   match = re.search(r'(\d{4}-\d{2}-\d{2})', path_name)
   date = datetime.datetime.strptime(match.group(), '%Y-%m-%d').date()
   df = pd.read_csv(f, low_memory=False)
   df = check_header(df, electionList, regInfo)
   df["build_date"] = date
   df.to_csv("/Users/Chris/Dropbox/batch_upload/fix_headers/c" + path_name)


```
In the **fix_headers** folder, the constituent files all have a **c** prefix. Thus, the data in its original form are located in the dropbox folder and google bucket.

The cleaned data are then uploaded to Google Cloud Storage, they're then turned into a table in Big Query called **combined_0724**  It's relatively easy to move large datasets around here. It's also easy to query them using standard SQL queries.

One important aspect of the project is geocoding the data. I did this locally on ArcGIS. I do this by first extracting all unique addresses from the the data, and save the table as **masterAddresses**. I then download the data, geocode it, and upload it back to GCP and BQ. The table is called **geoCoded0724**. The SQL code to do this is below.


## Geocoding

```sql
CREATE OR REPLACE TABLE `az-voter-file.Data_AVP_001.masterRegistrants`AS(
SELECT
  registrant_id,
  (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' ||  Residence_State || ', ' || Residence_Zip) AS addressInVote,
FROM
  `az-voter-file.Data_AVP_001.combined_0724`
  WHERE status = "Active"
);
CREATE OR REPLACE TABLE `az-voter-file.Data_AVP_001.masterAddresses`AS(
SELECT
  distinct(addressInVote),
FROM
  `az-voter-file.Data_AVP_001.masterRegistrants`
);
```

This is relatively simple. Now I just download the data, save it as a CSV, and process in ArcGIS.

## Download, Save, Use ArcGiS
```
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = XXX
#import tensorflow as tf
import os
import pandas as pd
from datetime import datetime
import pandas_gbq
from google.cloud import bigquery
from sklearn.preprocessing import MinMaxScaler
import pandas_gbq


query = """
SELECT
 *
FROM
  `az-voter-file.Data_AVP_001.masterAddresses`
"""
df = pandas_gbq.read_gbq(query, project_id="az-voter-file")
df.head()
df.to_csv('to_geocode.csv') ## For later load, not to sync.
```

This file is called **geocode.py**.



After geocoding, I upload back to GCP -- the file is called MASTER_ADDRESSES_072423, BQ and create a table called *geoCoded0724*

Here is a recap thus far:

1) Data is delivered in CSV files. I clean the headers and save the files in a folder called **fix_headers**. I then upload the files to GCP and Big Query, to a table called **combined_0724**
2) I then extract all unique addresses from the data, and save the table as **masterAddresses**. I then download the data, geocode it, and upload it back to GCP (**MASTER_ADDRESSES_072423**) and BQ. The table is called **geoCoded0724**.


* <span style="color: red;">combined_0724</span>: This is the "rawest form of the data. It shouldn't be altered. It's built from the CSV files." From here,

* <span style="color: grey;">masterRegistrants</span> : All grey entries indicate a "view" that is created in BQ. This view creates a set of master addresses from the <span style="color: red;">combined_0724</span> data

* <span style="color: grey;">masterAddresses</span> :This view creates a set of master addresses from the <span style="color: grey;">masterRegistrants</span> data.

* From here, I download the address file and geocode in ArcGIS. I then upload the file back to GCP and Big Query, to a file called <span style="color: red;">geocoded_0724</span>

* <span style="color: purple;">fullData</span> :This is a table. It's the full dataset, with addresses. It creates a set of master addresses from the <span style="color: red;">combined_0724</span> data

* <span style="color: grey;">fullDataGeo</span> :This is a view. It's the full dataset, with addresses. It creates a set of master addresses from the <span style="color: red;">combined_0724</span> data

* <span style="color: pink;">comp</span> :This is a function. It processes movement to different addresses.

* <span style="color: grey;">changeAddress</span> :This view generates a series of comparisons, then a table from the <span style="color: pink;">comp</span> function.

* <span style="color: grey;">oldVote</span> :This view pulls in observations from older elections. They aren't in the most recent build of the data.

* <span style="color: grey;">voteDataActionable</span> :This view merges older observations onto the newer data table.

* <span style="color: grey;">voteDataActionablePr</span> :This view geocodes voters into precincts. This isn't altogether necessary, except to make comparisons over time.

* <span style="color: black;">goldAddressError</span> :This corrects incorrectly geocoded stuff. It's more or less a utility, in that it creates fields that geocode people into districts, for comparisons to the state.

* <span style="color: grey;">dataForMLraw</span> :This just cleans and adds fields form the <span style="color: black;">goldAddressError</span> table.

* <span style="color: black;">dataForML</span> : Grabs some data from the census and creates a penultimate file for analysis, ML, etc.

------


```sql
-- This is costly
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.masterRegistrants` AS(
  SELECT
    registrant_id,
    (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' || Residence_State || ', ' || Residence_Zip) AS addressInVote,
  FROM
    `az-voter-file.AVPv2.combined_0724`
  WHERE
    status = "Active" );
----------
--   (2)
----------
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.masterAddresses`AS(
  SELECT
    DISTINCT(addressInVote),
  FROM
    `az-voter-file.AVPv2.masterRegistrants` );
  -- Merge the geocoded data with the most recent build of the data, here 07-24
----------
--   (3)
----------
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.fullData`AS(
  SELECT
    *,
    (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' || Residence_State || ', ' || Residence_Zip) AS addressInVote,
    Registrant_ID AS registrantID
  FROM
    `az-voter-file.AVPv2.combined_0724` );
    ----------
--   (4)
----------

CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.fullDataGeo`AS(
  SELECT
    SAFE_CAST(Score AS FLOAT64) AS scores,
    a.Match_type AS matchType,
    SAFE_CAST(X AS float64) AS longitude,
    SAFE_CAST(Y AS float64) AS latitude,
    b.*,
  FROM
    `az-voter-file.Data_AVP_001.geoCoded_0724` AS a
  RIGHT JOIN
    `az-voter-file.Data_AVP_001.fullData` AS b
  ON
    a.USER_addressInVote = b.addressInVote
  WHERE
    b.Status = "Active" );
  -- This generally does the right thing, work in the full data build workflow`
----------
--   (5)
----------

CREATE OR REPLACE TABLE
  FUNCTION `az-voter-file.AVPv2.comp`(d1 STRING,
    d2 STRING) AS(
  WITH
    fullGeo AS(
    SELECT
      SAFE_CAST(Score AS FLOAT64) AS scores,
      a.Match_type AS matchType,
      SAFE_CAST(a.X AS float64) AS longitude,
      SAFE_CAST(a.Y AS float64) AS latitude,
      ST_GEOGPOINT(SAFE_CAST(X AS float64), SAFE_CAST(Y AS float64)) AS geoPoint,
      b.*,
    FROM
      `az-voter-file.AVPv2.geoCoded_0724` AS a
    RIGHT JOIN
      `az-voter-file.AVPv2.fullData` AS b
    ON
      a.USER_addressInVote = b.addressInVote),
    tableA AS (
    SELECT
      longitude,
      latitude,
      Precinct,
      Registrant_ID AS id,
      (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' || Residence_State || ', ' || Residence_Zip) AS address,
    FROM
      fullGeo
    WHERE
      build_date = d1
      AND Status = "Active"),
    tableB AS (
    SELECT
      longitude,
      latitude,
      Precinct,
      Registrant_ID AS id,
      (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' || Residence_State || ', ' || Residence_Zip) AS address,
    FROM
      fullGeo
    WHERE
      build_date = d2
      AND Status = "Active")
  SELECT
    a.*,
    CASE
      WHEN a.address = b.address THEN 'no change'
    ELSE
    b.address
  END
    AS new_address,
    CASE
      WHEN a.longitude = b.longitude THEN NULL
    ELSE
    b.longitude
  END
    AS new_longitude,
    CASE
      WHEN a.latitude = b.latitude THEN NULL
    ELSE
    b.latitude
  END
    AS new_latitude,
  FROM
    tableA AS a
  JOIN
    tableB AS b
  ON
    a.id = b.id );
  ----------
--   (6)
----------

CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.changeAddress`AS (
  SELECT
    a.id,
    a.longitude AS longitudeRecent,
    a.latitude AS latitudeRecent,
    a.address AS address_recent,
    b.new_address AS c1122,
    b.new_latitude AS latc1122,
    b.new_longitude AS lonc1122,
    c.new_address AS c0922,
    c.new_latitude AS latc0922,
    c.new_longitude AS lonc0922,
    d.new_address AS c0522,
    d.new_latitude AS latc0522,
    d.new_longitude AS lonc0522,
    e.new_address AS c0421,
    e.new_latitude AS latc0421,
    e.new_longitude AS lonc0421,
    f.*
  FROM
    `az-voter-file.Data_AVP_001.comp`('2023-03-31',
      '2022-11-04') AS a
  LEFT JOIN
    `az-voter-file.Data_AVP_001.comp`('2023-03-31',
      '2022-10-28') AS b
  ON
    a.id = b.id
  LEFT JOIN
    `az-voter-file.Data_AVP_001.comp`('2023-03-31',
      '2022-09-30') AS c
  ON
    a.id = c.id
  LEFT JOIN
    `az-voter-file.Data_AVP_001.comp`('2023-03-31',
      '2022-05-03') AS d
  ON
    a.id = d.id
  LEFT JOIN
    `az-voter-file.Data_AVP_001.comp`('2023-03-31',
      '2021-04-28') AS e
  ON
    a.id = e.id
  RIGHT JOIN
    `az-voter-file.AVPv2.fullDataGeo` AS f
  ON
    a.id = f.Registrant_ID );
----------
--   (7)
----------
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.oldVote`AS(
  SELECT
    Registrant_ID AS registrantID,
    __08_2016_2016_GENERAL_ELECTION AS general_2016,
    __06_2018_2018_GENERAL_ELECTION AS general_2018,
    __28_2018_2018_PRIMARY_ELECTION AS primary_2018,
    __30_2016_2016_PRIMARY_ELECTION AS priamry_2016,
  FROM
    `az-voter-file.AVPv2.fullData`
  WHERE
    build_date = '2022-09-30'
    AND Status = "Active" );
  -- Combine the two, this is called voteDataActionable.,
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.voteDataActionable`AS(
  SELECT
    a.* EXCEPT(registrantID),
    b.*
  FROM
    `az-voter-file.AVPv2.oldVote` AS a
  RIGHT JOIN
    `az-voter-file.AVPv2.changeAddress` AS b
  ON
    a. registrantID = b.registrantID );
----------
--   (8)
----------

CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.voteDataActionablePr`AS(
  WITH
    precinct_shape AS(
    SELECT
      DISTINCT precinct,
      geo
    FROM
      `az-voter-file.AVPv2.precinct_vote_returns`
    ORDER BY
      precinct )
  SELECT
    a.*,
    a.precinct AS GeoPrecinct
  FROM
    `az-voter-file.AVPv2.voteDataActionable` AS a
  );
  ----------
--   (9)
----------

CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.goldAddressError`AS(
  WITH
    geo_address AS (
    SELECT
      *,
      ST_GEOGPOINT(SAFE_CAST(longitude AS float64), SAFE_CAST(latitude AS float64) ) AS location_in_address_file,
    FROM
      `az-voter-file.AVPv2.voteDataActionablePr`),
    lds AS (
    SELECT
      updates.*,
      geo.cd AS ldInGeometry,
      -- Put the geometry files in a common place
    FROM
      geo_address AS updates
    CROSS JOIN
      `az-voter-file.AVPv2.geo_legislative_districts` AS geo
    WHERE
      ST_CONTAINS(ST_GEOGFROMGEOJSON(geo.geo),updates.location_in_address_file)),
    cds AS (
    SELECT
      lds.*,
      geo.cd AS cdInGeometry,
      -- Put the geometry files in a common place
    FROM
      lds AS lds
    CROSS JOIN
      `az-voter-file.AVPv2.cd_geometries` AS geo
    WHERE
      ST_CONTAINS(ST_GEOGFROMGEOJSON(geo.geo),lds.location_in_address_file))
  SELECT
    cds.*,
    bg.geo_id AS census_block,
  FROM
    cds
  CROSS JOIN
    `bigquery-public-data.geo_census_blockgroups.blockgroups_04` AS bg
  WHERE
    ST_CONTAINS(bg.blockgroup_geom, cds.location_in_address_file) );
  ----------
--   (10)
----------

CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.dataForMLraw` AS (
  SELECT
    *,
    2022 - CAST(year_of_birth AS int) AS respondent_age,
    CAST(PEVL AS string) AS early_list,
    CAST(REGEXP_EXTRACT(congressional, r"([0-9]+)") AS string) AS CD,
    CAST(REGEXP_EXTRACT(legislative, r"([0-9]+)") AS string) AS LD,
    CASE
      WHEN party = "REP" THEN "Republican (REP)"
      WHEN party = "DEM" THEN "Democrat (DEM)"
    ELSE
    "Independent"
  END
    AS party_identification,
    CASE
      WHEN `priamry_2016` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS PRIMARY_e2016,
    CASE
      WHEN `general_2016` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS GENERAL_e2016,
    CASE
      WHEN `primary_2018` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS PRIMARY_e2018,
    CASE
      WHEN `general_2018` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS GENERAL_e2018,
    CASE
      WHEN `__04_2020_PRIMARY_2020` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS PRIMARY_e2020,
    CASE
      WHEN `__03_2020_GENERAL_2020` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS GENERAL_e2020,
    CASE
      WHEN `__02_2022_PRIMARY_2022` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS PRIMARY_e2022,
    CASE
      WHEN `__08_2022_GENERAL_2022` IS NOT NULL THEN 1
    ELSE
    0
  END
    AS GENERAL_e2022,
    COALESCE(SAFE_CAST(REGEXP_CONTAINS(`__08_2022_GENERAL_2022`, "E") AS int64), 0) AS early_voter,
    COALESCE(CAST(REGEXP_CONTAINS(`__08_2022_GENERAL_2022`, "A") AS int64), 0) AS absentee_voters,
    COALESCE(CAST(REGEXP_CONTAINS(`__08_2022_GENERAL_2022`, "PV") AS int64), 0) AS provisional_voters,
    COALESCE(CAST(REGEXP_CONTAINS(`__08_2022_GENERAL_2022`, "P") AS int64), 0) AS polling_voters,
    COALESCE(CAST(REGEXP_CONTAINS(`priamry_2016`, "REP") AS int64), 0) AS Republican_Primary_2016,
    COALESCE(CAST(REGEXP_CONTAINS(`priamry_2016`, "REP") AS int64), 0) AS Republican_Primary_2018,
    COALESCE(CAST(REGEXP_CONTAINS(`__04_2020_PRIMARY_2020`, "REP") AS int64), 0) AS Republican_Primary_2020,
    COALESCE(CAST(REGEXP_CONTAINS(`__02_2022_PRIMARY_2022`, "REP") AS int64), 0) AS Republican_Primary_2022,
    COALESCE(CAST(REGEXP_CONTAINS(`primary_2018`, "DEM") AS int64), 0) AS Democratic_Primary_2016,
    COALESCE(CAST(REGEXP_CONTAINS(`primary_2018`, "DEM") AS int64), 0) AS Democratic_Primary_2018,
    COALESCE(CAST(REGEXP_CONTAINS(`__04_2020_PRIMARY_2020`, "DEM") AS int64), 0) AS Democratic_Primary_2020,
    COALESCE(CAST(REGEXP_CONTAINS(`__02_2022_PRIMARY_2022`, "DEM") AS int64), 0) AS Democratic_Primary_2022,
  FROM
    `az-voter-file.AVPv2.goldAddressError` );
----------
--   (11)
----------

  CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.dataForML`AS(
  SELECT
    a.*,
    b.bachelors_degree,
    b.median_age,
    b.median_income,
    b.white_pop,
    b.black_pop,
    b.asian_pop,
    b.amerindian_pop,
    b.hispanic_pop,
    b.housing_units,
    b.armed_forces,
    b.employed_pop,
    b.pop_in_labor_force,
    b.total_pop,
    b.mobile_homes,
  FROM
    `az-voter-file.AVPv2.dataForMLraw` AS a
  LEFT JOIN
    `bigquery-public-data.census_bureau_acs.blockgroup_2018_5yr` AS b
  ON
    a.census_block = b.geo_id );
SELECT
  Registrant_ID,
  Registration_Date,
  Year_of_Birth,
  Effective_Date_of_Change,
  census_block,
  CD,
  LD,
  party_identification,
  GENERAL_e2016,
  GENERAL_e2016,
  GENERAL_e2018,
  GENERAL_e2020,
  PRIMARY_e2016,
  PRIMARY_e2018,
  PRIMARY_e2020,
  PRIMARY_e2022,
  GENERAL_e2022,
  early_voter,
  absentee_voters,
  provisional_voters,
  polling_voters,
  Republican_Primary_2022,
  Democratic_Primary_2022,
  Republican_Primary_2018,
  Democratic_Primary_2018,
  SAFE_DIVIDE(bachelors_degree, total_pop) as bachelors_degree,
  median_age,
  median_income,
  County,
  SAFE_DIVIDE(white_pop,total_pop) as white_pop,
  SAFE_DIVIDE(hispanic_pop,total_pop) as hispanic_pop,
  SAFE_DIVIDE(employed_pop,total_pop) as employed_pop,
  SAFE_DIVIDE(black_pop, total_pop) as black_pop,
  SAFE_DIVIDE(asian_pop, total_pop) as asian_pop,
  SAFE_DIVIDE(mobile_homes, total_pop) as mobile_homes,
  SAFE_DIVIDE(amerindian_pop, total_pop) as amerindian_pop,
FROM
  `az-voter-file.AVPv2.dataForML`

  ```


 # Download Data, Analysis
 Use VSCode or Rstudio or something or whatever to pull in to local machine. This file is called *download.py*


  ## Data

  The data can be pulled from a BQ table. I do this here, and save the files to my machine.
 ```{python}
# Formulate the SQL query to pull the data from BigQuery

# import tensorflow as tf
import os
import pandas as pd
from datetime import datetime
import pandas_gbq
from google.cloud import bigquery
from sklearn.preprocessing import MinMaxScaler
import pandas_gbq
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/Chris/Dropbox/Keys/az-voter-file-30395362c45b.json"

query = """
SELECT
  Registrant_ID,
  Year_of_Birth,
  Effective_Date_of_Change,
  census_block,
  CD,
  LD,
  party_identification,
  GENERAL_e2016,
  GENERAL_e2016,
  GENERAL_e2018,
  GENERAL_e2020,
  PRIMARY_e2016,
  PRIMARY_e2018,
  PRIMARY_e2020,
  PRIMARY_e2022,
  GENERAL_e2022,
  early_voter,
  absentee_voters,
  provisional_voters,
  polling_voters,
  Republican_Primary_2022,
  Democratic_Primary_2022,
  Republican_Primary_2018,
  Democratic_Primary_2018,
  SAFE_DIVIDE(bachelors_degree, total_pop) as bachelors_degree,
  median_age,
  median_income,
  County,
  SAFE_DIVIDE(white_pop,total_pop) as white_pop,
  SAFE_DIVIDE(hispanic_pop,total_pop) as hispanic_pop,
  SAFE_DIVIDE(employed_pop,total_pop) as employed_pop,
  SAFE_DIVIDE(black_pop, total_pop) as black_pop,
  SAFE_DIVIDE(asian_pop, total_pop) as asian_pop,
  SAFE_DIVIDE(mobile_homes, total_pop) as mobile_homes,
  SAFE_DIVIDE(amerindian_pop, total_pop) as amerindian_pop,
FROM
  `az-voter-file.AVPv2.dataForML`
    WHERE build_date = "2023-03-31"
"""


df = pandas_gbq.read_gbq(query, project_id="az-voter-file")
df.head()


# For later load, not to sync.
df.to_pickle('/Users/Chris/Dropbox/masterData/voterFile/voterActionable.pkl')
df.to_csv('/Users/Chris/Dropbox/masterData/voterFile/voterActionable.csv')  #

 ```

 ## Building a Voter Engagement Score
Process the data and run a latent variable model. This file is called **latent.r**

 ```{r}
library(lavaanPlot)
library(lavaan)
library(semTools)
library(dplyr)
library(brms)
library(ggplot2)
library(tidybayes)
library(cowplot)

df <- read.csv("/Users/Chris/Dropbox/github_repos/the_az_voter_project/the_az_voter_project/wrangling/voter_file.csv")
head(df)
# # Fit Mimic Model
df$Republican <- ifelse(df$party_identification == "Republican (REP)", 1, 0)
df$Democratic <- ifelse(df$party_identification == "Democrat (DEM)", 1, 0)
df$power <- rowSums(cbind(
  df$GENERAL_e2016, df$GENERAL_e2018, df$PRIMARY_e2016,
  df$PRIMARY_e2018, df$GENERAL_e2020, df$PRIMARY_e2020
), na.rm = TRUE)
df$rep_leaner <- ifelse((df$Republican_Primary_2022 == 1), 1, 0)
df$dem_leaner <- ifelse((df$Democratic_Primary_2022 == 1), 1, 0)

# factor_vars <- c("GENERAL_e2016", "GENERAL_e2018", "GENERAL_e2020")
# paste0(paste0(factor_vars, collapse = " + "), " + Rep:", paste0(factor_vars, collapse = " + Republican*"), " + ", "Democratic:", paste0(factor_vars, collapse = " + Democratic*"))

# Misc Functions
zeroOne <- function(x) {
  min.x <- min(x, na.rm = T)
  max.x <- max(x - min.x, na.rm = T)
  return((x - min.x) / max.x)
}
sem_model <- function(data, factor_vars, name = "general") {
  model <- paste0("
    f0 =~ ", paste0(factor_vars, collapse = " + "), "\n", "f0 ~ Republican + Democratic", "\n")

  fit <- sem(model, data = data)

  ids <- lavInspect(fit, "case.idx")
  factor.scores <- lavPredict(fit)
  for (fs in colnames(factor.scores)) {
    data[, fs] <- factor.scores[, fs]
  }
  # df$primary_voter = zeroOne(df$primary_voter)
  data[[name]] <- zeroOne(data$f0)
  data <- data %>% select(-f0)
  return(data)
}


factor_vars <- c("GENERAL_e2016", "GENERAL_e2018", "GENERAL_e2020", "GENERAL_e2022")
df <- sem_model(data = df, factor_vars = factor_vars, name = "generalV")
factor_vars <- c("GENERAL_e2016", "GENERAL_e2018", "GENERAL_e2020")
df <- sem_model(data = df, factor_vars = factor_vars, name = "generalLag")
factor_vars <- c("PRIMARY_e2016", "PRIMARY_e2018", "PRIMARY_e2020", "PRIMARY_e2022")
df <- sem_model(data = df, factor_vars = factor_vars, name = "primaryV")
factor_vars <- c("PRIMARY_e2016", "PRIMARY_e2018", "PRIMARY_e2020")
df <- sem_model(data = df, factor_vars = factor_vars, name = "primaryLag")


df %>%
  group_by(census_block) %>%
  mutate(voterCount = n()) %>%
  mutate(blockDem = mean(Democratic)) %>%
  mutate(blockRep = mean(Republican)) %>%
  mutate(blockOther = 1 - blockDem - blockRep) %>%
  mutate(blockDemleaner = mean(dem_leaner)) %>%
  mutate(blockRepleaner = mean(rep_leaner)) %>%
  ungroup() %>%
  write.csv("voter_file_latent_.csv", row.names = FALSE)

```

 And then process the data again in python, run some simple ML models using tensorflow, construct the data, and output a file called **eng_pred_localscores.csv**. The python file that runs everything below is called **ml_upload.py**

```{python}
from tensorflow.keras.metrics import Accuracy, Precision, Recall
from tf.keras.optimizers.legacy import Adam
from tensorflow.keras.regularizers import l1_l2
import numpy as np
import tensorflow as tf
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
import keras
from keras import layers

dataForML = pd.read_csv(
    '/Users/Chris/Dropbox/github_repos/the_az_voter_project/the_az_voter_project/local_files/voter_file_latent_.csv')


dataForML["Lean_Democrat"] = np.where((dataForML["party_identification"] == 'Independent') & (
    dataForML['Democratic_Primary_2022'] == 1), 1, 0)
dataForML["Lean_Republican"] = np.where((dataForML["party_identification"] == 'Independent') & (
    dataForML['Republican_Primary_2022'] == 1), 1, 0)
dataForML["Defect_to_Democrat"] = np.where((dataForML["party_identification"] == 'Republican (REP)') & (
    dataForML['Democratic_Primary_2022'] == 1), 1, 0)
dataForML["Defect_to_Republican"] = np.where((dataForML["party_identification"] == 'Democrat (DEM)') & (
    dataForML['Republican_Primary_2022'] == 1), 1, 0)
dataForML["Republican"] = np.where(
    (dataForML["party_identification"] == 'Democrat (DEM)'), 1, 0)
dataForML["Democratic"] = np.where(
    (dataForML["party_identification"] == 'Republican (REP)'), 1, 0)
dataForML["age"] = 2022 - dataForML["Year_of_Birth"]
dataForML["voting2022"] = np.where(
    (dataForML["PRIMARY_e2022"] == 1) | (dataForML["GENERAL_e2022"] == 1), 1, 0)
dataForML["primaryVoter"] = np.where((dataForML["PRIMARY_e2022"] == 1) |
                                     (dataForML["PRIMARY_e2020"] == 1) |
                                     (dataForML["PRIMARY_e2018"] == 1) |
                                     (dataForML["PRIMARY_e2016"] == 1), 1, 0)

IDS = ['Registrant_ID']
FEATURES = ["Republican", "Democratic",
            "dem_leaner", "rep_leaner",
            "hispanic_pop", "white_pop",
            "asian_pop", "bachelors_degree",
            "age", 'generalLag', 'primaryLag',
            'median_age', 'median_income',
            'Year_of_Birth']


LABELS = ["GENERAL_e2022"]
dat = dataForML[LABELS + FEATURES + IDS]
dat = dat.dropna(how='any')
registrant_ids = dat[IDS]
dat = dat[LABELS + FEATURES + IDS]
scaler = MinMaxScaler()
dat_array = scaler.fit_transform(dat)
dat = pd.DataFrame(dat_array, columns=dat.columns)


train, test = train_test_split(dat, test_size=0.10)
labels_train = pd.DataFrame(
    {"Voter": train["GENERAL_e2022"],  "notVoter": 1-train["GENERAL_e2022"]})
labels_test = pd.DataFrame(
    {"Voter": test["GENERAL_e2022"],   "notVoter": 1-test["GENERAL_e2022"]})


features_test = test[FEATURES]
features_train = train[FEATURES]
labels_train_array = np.array(labels_train, np.float64)
labels_test_array = np.array(labels_test,   np.float64)
features_train_array = np.array(features_train, np.float64)
features_test_array = np.array(features_test,   np.float64)

features_full = dat[FEATURES]
labels_full = pd.DataFrame(
    {"notVoter": dat["GENERAL_e2022"],  "notVoter": 1-dat["GENERAL_e2022"]})
labels_test = dat[LABELS]

labels_full_array = np.array(labels_full, np.float64)
features_full_array = np.array(features_full, np.float64)


modelNN = tf.keras.Sequential()
# Define the first layer
modelNN.add(keras.layers.Dense(15, activation='softmax',
                               input_shape=(features_train.shape[1],)))
modelNN.add(keras.layers.Dropout(0.25))
modelNN.add(keras.layers.Dense(12, activation='relu'))
modelNN.add(keras.layers.Dropout(0.25))
modelNN.add(keras.layers.Dense(8, activation='relu'))
modelNN.add(keras.layers.Dense(2, activation='softmax'))


# Finish the model compilation
modelNN.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001),
                loss='categorical_crossentropy',
                metrics=['accuracy'])

callback = tf.keras.callbacks.EarlyStopping(monitor='loss', patience=3)
print("TensorFlow version:", tf.__version__)
print("Num GPUs Available: ", len(
    tf.config.experimental.list_physical_devices('GPU')))
tf.config.list_physical_devices('GPU')

modelNN.fit(features_train_array,
            labels_train_array,
            epochs=20,
            batch_size=256,
            validation_split=0.10,
            callbacks=[callback],
            verbose=1)


# Evaluate the model on the test data using `evaluate`
print("Evaluate on test data")
results = modelNN.evaluate(
    features_test_array, labels_test_array, batch_size=128)
print("test loss, test acc:", results)
predictions = modelNN.predict(features_full_array)
predictions = pd.DataFrame(predictions)
predictions.head()


min_max_scalar = MinMaxScaler(feature_range=(-1, 1))

upload_data = pd.DataFrame({"Engaged_Voter_Prediction":  predictions.iloc[:, 0],
                            "Not_Engaged_Voter_Prediction":  predictions.iloc[:, 1],
                            # This will change it's a simulation
                            # Take a binomial draw from the prediction
                            "Registrant_ID": registrant_ids.iloc[:, 0],
                            "Lean_Democrat": dataForML["Lean_Democrat"].astype(int),
                            "Lean_Republican": dataForML["Lean_Republican"].astype(int),
                            "Defect_to_Republican": dataForML["Defect_to_Republican"].astype(int),
                            "primaryVoterScore": dataForML["primaryV"].astype(float),
                            "generalVoterScore": dataForML["generalV"].astype(float),
                            "primaryVoterLScore": dataForML["primaryLag"].astype(float),
                            "generalVoterLScore": dataForML["generalLag"].astype(float)
                            })
upload_data["primaryVoterScore"] = min_max_scalar.fit_transform(
    upload_data[["primaryVoterScore"]])
upload_data["generalVoterScore"] = min_max_scalar.fit_transform(
    upload_data[["generalVoterScore"]])
upload_data["primaryVoterLScore"] = min_max_scalar.fit_transform(
    upload_data[["primaryVoterLScore"]])
upload_data["generalVoterLScore"] = min_max_scalar.fit_transform(
    upload_data[["generalVoterLScore"]])

upload_data.to_csv('eng_pred_localscores.csv')

```

## Writing Data using the Census API

```{python}
import gspread
import re
import pandas as pd
import numpy as np
from google.colab import auth
from oauth2client.client import GoogleCredentials
from census import Census
from us import states

auth.authenticate_user()


c = Census(CENSUS API)
VARS = (["P1_" + str(i + 1).zfill(3) + "N"for i in range(45)] +
        ["P2_" + str(i + 1).zfill(3) + "N"for i in range(3)])

ld = pd.DataFrame.from_dict(c.pl.get((VARS), geo={'for': 'state legislative district (lower chamber):*',
                            'in': 'state:{}'.format(states.AZ.fips)}))
ld = ld.rename(columns={'state legislative district (lower chamber)': 'LD'})

cd = pd.DataFrame.from_dict(c.pl.get(VARS, geo={'for': 'congressional district:*',
                            'in': 'state:{}'.format(states.AZ.fips)}))
cd = cd.rename(columns={'congressional district': 'CD'})

county = pd.DataFrame.from_dict(c.pl.get(VARS, geo={'for': 'county:*',
                                                    'in': 'state:{}'.format(states.AZ.fips)}))
county_tract = pd.DataFrame.from_dict(c.pl.get(VARS, geo={'for': 'tract:*',
                                                          'in': 'state:{}. county:*'.format(states.AZ.fips)}))
county_block = pd.DataFrame.from_dict(c.pl.get(VARS, geo={'for': 'block:*',
                                                          'in': 'state:{}. county:*'.format(states.AZ.fips)}))

project_id = 'az-voter-file'

project_table = 'az-voter-file.Data_AVP_001.ld'
ld.to_gbq(project_table, project_id, chunksize=None, if_exists='replace')

project_table = 'az-voter-file.Data_AVP_001.cd'
cd.to_gbq(project_table, project_id, chunksize=None, if_exists='replace')

project_table = 'az-voter-file.Data_AVP_001.county'
county.to_gbq(project_table, project_id, chunksize=None, if_exists='replace')

project_table = 'az-voter-file.Data_AVP_001.county_tract'
county_tract.to_gbq(project_table, project_id,
                    chunksize=None, if_exists='replace')

project_table = 'az-voter-file.Data_AVP_001.county_block'
county_block.to_gbq(project_table, project_id,
                    chunksize=None, if_exists='replace')

```
* <span style="color: red;">writeCensus.py</span> : This writes census data for analysis: <span style="color: grey;">ld, cd, county, county_block, county_tract</span>

## Data for Visualization and Further Analysis

**eng_pred_localscores.csv** is uploaded to a Google Bucket. I then create a BQ table called <span style="color: red;">fullDataPostML</span>.

---
#### <span style="color: red;">External Data</span>. Files uploaded to GCP and then manually created in BQ.
#### <span style="color: grey;">Views</span> : Big Query views save storage by not actually saving a table.
#### <span style="color: black;">Tables</span> : Queries that create tables.
#### <span style="color: purple;">Functions</span> : Table functions.

-----


* <span style="color: black;">data_merged</span> :This is a table. It's the full dataset, with addresses. It creates a set of master addresses from the <span style="color: red;">fullDataPostML</span> data

* <span style="color: grey;">datReduced</span> : I grab some stuff from <span style="color: black;">data_merged</span>, which is rather large -- but we'd like flexibility to download whole thing.

* <span style="color: grey;">centroids</span> : This creates a table of centroids for each census block.

* <span style="color: black;">starTable</span> : This creates a table of counts from the voter data. The counts are by block, county, LD, CD, and party identification.

* <span style="color: black;">starTableCent</span> : The star table with centroids.

### Census Data

* <span style="color: black;">block_aggregates, county_aggregates, ldAggregates, cdAggregates, </span> : These are taken from the census.

### Census Data

* <span style="color: black;">starAggregates</span> : The star table with the summary census information.


```{sql}
       -- - Post ML Upload --
      -- Construct the census blocks from the neural networks
      -- This creates an aggregated score.
   CREATE OR REPLACE TABLE
     `az-voter-file.AVPv2.data_merged` AS(
      with tableA as (
    SELECT
       *
     FROM  `az-voter-file.AVPv2.fullDataPostML`
  ),
  tableB as(
    SELECT
  *
  FROM  `az-voter-file.AVPv2.dataForML`
  WHERE build_date = "2023-03-31"
  )
  SELECT
  tableA.*,
  tableB.* except(Registrant_ID),
  FROM tableA  LEFT JOIN tableB ON
   CAST(tableA.Registrant_ID AS int) = CAST(tableB.registrant_id AS int)
   );
  -- This generates a table of approximately 3.56 million voters
  CREATE OR REPLACE VIEW
    `az-voter-file.AVPv2.datReduced`AS(
    SELECT
      census_block,
      County,
      Engaged_Voter_Prediction,
      Not_Engaged_Voter_Prediction,
      Registrant_ID,
      Lean_Democrat,
      Lean_Republican,
      Defect_to_Republican,
      primaryVoterScore,
      generalVoterScore,
      primaryVoterLScore,
      generalVoterLScore,
      CD,
      LD,
      longitude,
      latitude,
      Precinct,
      registrantID,
      location_in_address_file,
      GeoPrecinct,
      ldInGeometry cdInGeometry,
      respondent_age,
      party_identification,
      PRIMARY_e2016,
      GENERAL_e2016,
      PRIMARY_e2018,
      GENERAL_e2018,
      PRIMARY_e2020,
      GENERAL_e2020,
      PRIMARY_e2022,
      GENERAL_e2022,
      early_voter,
      absentee_voters,
      provisional_voters,
      polling_voters,
      Republican_Primary_2016,
      Republican_Primary_2018,
      Republican_Primary_2020,
      Republican_Primary_2022,
      Democratic_Primary_2016,
      Democratic_Primary_2018,
      Democratic_Primary_2020,
      Democratic_Primary_2022,
      bachelors_degree,
      median_age,
      median_income,
      white_pop,
      black_pop,
      asian_pop,
      amerindian_pop,
      hispanic_pop,
      housing_units,
      armed_forces,
      employed_pop,
      pop_in_labor_force,
      total_pop,
      mobile_homes,
      CASE
        WHEN party_identification = "Republican (REP)" THEN 1
      ELSE
      0
    END
      AS Republican,
      CASE
        WHEN party_identification = "Democrat (DEM)" THEN 1
      ELSE
      0
    END
      AS Democrat,
      CASE
        WHEN party_identification <> "Republican (REP)" AND party_identification <> "Democrat (DEM)" THEN 1
      ELSE
      0
    END
      AS Independent
    FROM
      `az-voter-file.AVPv2.data_merged` );
   CREATE OR REPLACE VIEW `az-voter-file.AVPv2.centroids` AS(
       SELECT
         geo_id AS census_block,
         internal_point_lat AS centroid_latitude,
         internal_point_lon AS centroid_longitude,
       FROM
         `bigquery-public-data.geo_census_blockgroups.blockgroups_04` );
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.starTable`AS(
  SELECT
    census_block,
    County,
    party_identification AS pid,
    CAST(CD AS int64) AS CD,
    CAST(LD AS int64) AS LD,
    AVG(Engaged_Voter_Prediction) AS voterPrediction,
    SUM(Lean_Democrat) AS democraticLeaners,
    SUM(Lean_Republican) AS republicanLeaners,
    AVG(primaryVoterScore) AS primaryVoterScore,
    AVG(generalVoterScore) AS generalVoterScore,
    AVG(primaryVoterLScore) AS primaryVoterLScore,
    AVG(generalVoterLScore) AS generalVoterLScore,
    SUM(GENERAL_e2016) AS general2016,
    SUM(PRIMARY_e2016) AS primary2016,
    SUM(PRIMARY_e2018) AS primary2018,
    SUM(GENERAL_e2018) AS general2018,
    SUM(PRIMARY_e2020) AS primary2020,
    SUM(GENERAL_e2020) AS general2020,
    SUM(PRIMARY_e2022) AS primary2022,
    SUM(GENERAL_e2022) AS general2022,
    SUM(early_voter) AS early_voter,
    SUM(absentee_voters) AS absentee_voters,
    SUM(provisional_voters) AS provisional_voters,
    SUM(polling_voters) AS polling_voters,
    SUM(Democratic_Primary_2016) AS Democratic_Primary_2016,
    SUM(Democratic_Primary_2018) AS Democratic_Primary_2018,
    SUM(Democratic_Primary_2020) AS Democratic_Primary_2020,
    SUM(Democratic_Primary_2022) AS Democratic_Primary_2022,
    SUM(Republican_Primary_2016) AS Republican_Primary_2016,
    SUM(Republican_Primary_2018) AS Republican_Primary_2018,
    SUM(Republican_Primary_2020) AS Republican_Primary_2020,
    SUM(Republican_Primary_2022) AS Republican_Primary_2022,
    SUM(Republican) AS countRepublicans,
    SUM(Democrat) AS countDemocrats,
    SUM(Independent) AS countIndependents,
  FROM
    `az-voter-file.AVPv2.datReduced`
  GROUP BY
    census_block,
    County,
    CD,
    LD,
    pid );
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.starTableCent` AS(
  SELECT
    star.*,
    centroids.* EXCEPT(census_block)
  FROM
    `az-voter-file.AVPv2.starTable` AS star
  JOIN
    `az-voter-file.AVPv2.centroids` AS centroids
  ON
    star.census_block = centroids.census_block );
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.blockAggregates`AS(
 SELECT
    geo_id AS census_block_,
    total_pop,
    households,
    male_pop,
    female_pop,
    median_age,
    white_pop,
    pop_16_over,
    black_pop,
    asian_pop,
    hispanic_pop,
    amerindian_pop,
    other_race_pop,
    two_or_more_races_pop,
    not_hispanic_pop,
    median_income,
    income_per_capita,
    income_less_10000,
    income_10000_14999,
    income_15000_19999,
    income_20000_24999,
    income_25000_29999,
    income_30000_34999,
    income_35000_39999,
    income_40000_44999,
    income_45000_49999,
    income_50000_59999,
    income_60000_74999,
    income_75000_99999,
    income_100000_124999,
    income_125000_149999,
    income_150000_199999,
    income_200000_or_more,
    housing_units,
    occupied_housing_units,
    housing_units_renter_occupied,
    vacant_housing_units,
    vacant_housing_units_for_rent,
    vacant_housing_units_for_sale,
    mobile_homes,
    housing_built_2005_or_later,
    housing_built_2000_to_2004,
    housing_built_1939_or_earlier,
    median_year_structure_built,
    married_households,
    nonfamily_households,
    family_households,
    median_rent,
    percent_income_spent_on_rent,
    million_dollar_housing_units,
    aggregate_travel_time_to_work,
    commuters_by_public_transportation,
    associates_degree,
    bachelors_degree,
    high_school_diploma,
    less_one_year_college,
    masters_degree,
    one_year_more_college,
    employed_pop,
    unemployed_pop,
    centroids.centroid_longitude as centroid_longitude,
    centroids.centroid_latitude as centroid_latitude,
  FROM
    `bigquery-public-data.census_bureau_acs.blockgroup_2018_5yr` as a JOIN
    `az-voter-file.AVPv2.centroids` as centroids
  ON
    a.geo_id = centroids.census_block);
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.countyAggregates`AS(
  WITH
    util AS (
    SELECT
      county_fips_code,
      area_name,
      -- REGEXP_EXTRACT(county_fips_code, "\\d{3}$") AS county_fips,
      REGEXP_REPLACE(area_name, " County", "") AS county_name,
    FROM
      `bigquery-public-data.census_utility.fips_codes_all`
    WHERE
      state_fips_code = "04" )
  SELECT
    *
  FROM
    `az-voter-file.censusData.county` AS t1
  JOIN
    util AS t2
  ON
    t1.county_fips = t2.county_fips_code );
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.ldAggregates`AS(
  SELECT
total as legP1_001N,
population_one_race as legP1_002N,
white as legP1_003N,
black as legP1_004N,
native as legP1_005N,
asian as legP1_006N,
pacific as legP1_007N,
population_hispanic_total as legP2_001N,
hispanic as legP2_002N,
not_hispanic as legP2_003N,
ld as legislative_district
  FROM
    `az-voter-file.censusData.legislative_district04`);
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.cdAggregates`AS(
  SELECT
total as congP1_001N,
population_one_race as congP1_002N,
white as congP1_003N,
black as congP1_004N,
native as congP1_005N,
asian as congP1_006N,
pacific as congP1_007N,
population_hispanic_total as congP2_001N,
hispanic as congP2_002N,
not_hispanic as congP2_003N,
cd as congressional_district
FROM `az-voter-file.censusData.congressional_district04`);


CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.starAggregates`AS(
SELECT
* except(centroid_longitude, centroid_latitude)
     FROM
    `az-voter-file.AVPv2.starTableCent` AS star
  JOIN
    `az-voter-file.AVPv2.cdAggregates` AS cd
  ON
    star.CD = cast(cd.congressional_district as INT)
  JOIN
    `az-voter-file.AVPv2.ldAggregates` AS ld
  ON
    star.LD  = cast(ld.legislative_district as INT)
  JOIN
    `az-voter-file.AVPv2.countyAggregates` AS county
  ON
    cast(star.County as string) = county.county_name
  JOIN
    `az-voter-file.AVPv2.blockAggregates` AS block
  ON
    block.census_block_ = star.census_block
  );
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.blockAggregates`AS(
  WITH
    s1 AS (
    SELECT
      g1.*,
      cd_shapes.cd AS corrected_cd
    FROM
      `az-voter-file.AVPv2.blockAggregates` AS g1
    CROSS JOIN
      `az-voter-file.AVPv2.geo_cd` AS cd_shapes
    WHERE
      ST_CONTAINS(ST_GEOGFROMGEOJSON(cd_shapes.geo), ST_GEOGPOINT(g1.centroid_longitude, g1.centroid_latitude)) ),
    s2 AS (
    SELECT
      s1.*,
      ld_shapes.cd AS corrected_ld
    FROM
      s1
    CROSS JOIN
      `az-voter-file.AVPv2.ld_geo` AS ld_shapes
    WHERE
      ST_CONTAINS(ST_GEOGFROMGEOJSON(ld_shapes.geo), ST_GEOGPOINT(s1.centroid_longitude, s1.centroid_latitude)) ),
    s3 AS (
    SELECT
      s2.*,
      geo_county.id AS corrected_county
    FROM
      s2
    CROSS JOIN
      `az-voter-file.AVPv2.geo_county` AS geo_county
    WHERE
      ST_CONTAINS(ST_GEOGFROMGEOJSON(geo_county.geo), ST_GEOGPOINT(s2.centroid_longitude, s2.centroid_latitude)) )
  SELECT
    *
  FROM
    s3 )
```

## Public Opinion

```{R}
library(haven)
library(tidyverse)
path <- file.path("/Users/Chris/recodes_yougov/survey-2022-yougov/survey_recodes/data/AZ2022.sav")
df <- read_sav(path)
alot_4 <- list(`1` = 4, `2` = 3, `3` = 2, `4` = 1)
alot_2 <- list(`1` = 1, `2` = 1, `3` = 0, `4` = 0)

agree_5 <- list(`1` = 5, `2` = 4, `3` = 3, `4` = 2, `5` = 1)
agree_2 <- list(`1` = 1, `2` = 1, `3` = 0, `4` = 0, `5` = 0)
agree_5c <- list(`1` = 1, `2` = 2, `3` = 3, `4` = 4, `5` = 5)
agree_2c <- list(`1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)

agre_4 <- list(`1` = 4, `2` = 3, `3` = 2, `4` = 1)
agre_2 <- list(`1` = 1, `2` = 1, `3` = 0, `4` = 0)

agre_4c <- list(`1` = 1, `2` = 2, `3` = 3, `4` = 4)
agre_2c <- list(`1` = 0, `2` = 0, `3` = 1, `4` = 1)


df <- df %>%
  mutate(age = 2022 - birthyr) %>%
  # exclude inegligibles
  mutate(vote_2020 = car::recode(as.numeric(vote_2020), "1=1; 2=0; else = NA")) %>%
  mutate(vote_trump = car::recode(as.numeric(presvote20post), "1=0; 2=1; else = NA")) %>%
  mutate(vote_2022 = car::recode(as.numeric(vote_2022), "1=1; 2=0; else = NA")) %>%
  # Only 2 people report not voting.
  mutate(vote_2022_type = car::recode(as.numeric(df$vote), "1=1; 2=2; 3=3; 4=4; else = NA")) %>%
  # Always code republican as 1, democrat as 0.
  mutate(vote_senate = car::recode(as.numeric(general_senate), "1=0; 2=1; else = NA")) %>%
  mutate(vote_gov = car::recode(as.numeric(general_gov), "1=0; 2=1; else = NA")) %>%
  mutate(vote_sos = car::recode(as.numeric(SoS), "1=0; 2=1; else = NA")) %>%
  mutate(primary_type = car::recode(as.numeric(primary), "1=1; 2=2; 3=3; 4=4; else = NA")) %>%
  # 1= Rep ; 0 = Dem
  mutate(primary_type = car::recode(as.numeric(primary_rep), "1=1; 2=0; else = NA")) %>%
  mutate(primary_republican_gov = car::recode(as.numeric(primary_dem), "1= 1; 2=2; 3:4 = 3; else = NA")) %>%
  mutate(primary_democratic_gov = car::recode(as.numeric(primary_dem_gov), "1= 1; 2=2; 3 = 3; 4 = 4; else = NA")) %>%
  mutate(primary_republican_senate = car::recode(as.numeric(primary_rep_senate), "1=1; 2=2; 3 = 3; 4:5 = 4; else = NA")) %>%
  # Rank order questions, 1 counts
  mutate(senate_alignment = ifelse(rank_order_senate_1 == 1, 1, 0)) %>%
  mutate(senate_ofparty = ifelse(rank_order_senate_2 == 1, 1, 0)) %>%
  mutate(senate_likedthem = ifelse(rank_order_senate_3 == 1, 1, 0)) %>%
  mutate(senate_dislikedother = ifelse(rank_order_senate_4 == 1, 1, 0)) %>%
  mutate(senate_thepast = ifelse(rank_order_senate_5 == 1, 1, 0)) %>%
  mutate(governor_alignment = ifelse(rank_order_governor_1 == 1, 1, 0)) %>%
  mutate(governor_ofparty = ifelse(rank_order_governor_2 == 1, 1, 0)) %>%
  mutate(governor_likedthem = ifelse(rank_order_governor_3 == 1, 1, 0)) %>%
  mutate(governor_dislikedother = ifelse(rank_order_governor_4 == 1, 1, 0)) %>%
  mutate(governor_thepast = ifelse(rank_order_governor_5 == 1, 1, 0)) %>%
  mutate(sos_alignment = ifelse(rank_order_SoS_1 == 1, 1, 0)) %>%
  mutate(sos_ofparty = ifelse(rank_order_SoS_2 == 1, 1, 0)) %>%
  mutate(sos_likedthem = ifelse(rank_order_SoS_3 == 1, 1, 0)) %>%
  mutate(sos_dislikedother = ifelse(rank_order_SoS_4 == 1, 1, 0)) %>%
  mutate(sos_thepast = ifelse(rank_order_SoS_5 == 1, 1, 0)) %>%
  mutate(Biden_FT = Biden / 100) %>%
  # Biden, Trump, Lake, Masters, Hobbs, Kelly, Finchem, Fontes, MAGARep, EstabRep, ProgDem, EstabDem
  mutate(Trump_FT = Trump / 100) %>%
  mutate(Lake_FT = Lake / 100) %>%
  mutate(Masters_FT = Masters / 100) %>%
  mutate(Hobbs_FT = Hobbs / 100) %>%
  mutate(Kelly_FT = Kelly / 100) %>%
  mutate(Finchem_FT = Finchem / 100) %>%
  mutate(Fontes_FT = Fontes / 100) %>%
  mutate(MAGARep_FT = MAGARep / 100) %>%
  mutate(EstabRep_FT = EstabRep / 100) %>%
  mutate(ProgDem_FT = ProgDem / 100) %>%
  mutate(EstabDem_FT = EstabDem / 100) %>%
  mutate(biden_economy_4 = recode(as.numeric(biden_economy), !!!alot_4)) %>%
  mutate(biden_economy_2 = recode(as.numeric(biden_economy), !!!alot_2)) %>%
  mutate(personal_economy_4 = recode(as.numeric(personal_economy), !!!alot_4)) %>%
  mutate(personal_economy_2 = recode(as.numeric(personal_economy), !!!alot_2)) %>%
  # Causes of Price increase.
  mutate(cause_covid_4 = recode(as.numeric(causes1), !!!alot_4)) %>%
  mutate(cause_covid_2 = recode(as.numeric(causes1), !!!alot_2)) %>%
  mutate(cause_russia4 = recode(as.numeric(causes2), !!!alot_4)) %>%
  mutate(cause_russia2 = recode(as.numeric(causes2), !!!alot_2)) %>%
  mutate(cause_spending4 = recode(as.numeric(causes3), !!!alot_4)) %>%
  mutate(cause_spending2 = recode(as.numeric(causes3), !!!alot_2)) %>%
  mutate(cause_supply4 = recode(as.numeric(causes4), !!!alot_4)) %>%
  mutate(cause_supply2 = recode(as.numeric(causes4), !!!alot_2)) %>%
  mutate(cause_dems4 = recode(as.numeric(causes5), !!!alot_4)) %>%
  mutate(cause_dems2 = recode(as.numeric(causes5), !!!alot_2)) %>%
  mutate(cause_interest4 = recode(as.numeric(causes6), !!!alot_4)) %>%
  mutate(cause_interest2 = recode(as.numeric(causes6), !!!alot_2)) %>%
  ## Immigration hurt by califrno
  mutate(cali_5 = recode(as.numeric(cali), !!!agree_5)) %>%
  mutate(cali_2 = recode(as.numeric(cali), !!!agree_2)) %>%
  mutate(intern_5 = recode(as.numeric(Q49), !!!agree_5)) %>%
  mutate(intern_2 = recode(as.numeric(Q49), !!!agree_2)) %>%
  mutate(immig_5 = recode(as.numeric(immiga), !!!agree_5)) %>%
  mutate(immig_5 = recode(as.numeric(immiga), !!!agree_2)) %>%
  mutate(separate_parents_5 = recode(as.numeric(immig1), `1` = 1, `2` = 2, `3` = 3, `4` = 4, `5` = 5)) %>%
  mutate(separate_parents_2 = recode(as.numeric(immig1), `1` = 0, `2` = 0, `3` = 1, `4` = 1)) %>%
  mutate(legal_status_4 = recode(as.numeric(immig2), `1` = 1, `2` = 2, `3` = 3, `4` = 4)) %>%
  mutate(legal_status_2 = recode(as.numeric(immig2), `1` = 0, `2` = 0, `3` = 1, `4` = 1)) %>%
  mutate(citizen_4 = recode(as.numeric(immig3), `1` = 1, `2` = 2, `3` = 3, `4` = 4)) %>%
  mutate(citizen_2 = recode(as.numeric(immig3), `1` = 0, `2` = 0, `3` = 1, `4` = 1)) %>%
  mutate(smart_4 = recode(as.numeric(immig3), `1` = 4, `2` = 3, `3` = 2, `4` = 1)) %>%
  mutate(smart_2 = recode(as.numeric(immig3), `1` = 1, `2` = 1, `3` = 0, `4` = 0)) %>%
  ### Conservation is scored higher
  mutate(water_supply_4 = recode(as.numeric(water1), !!!agre_4c)) %>%
  mutate(water_supply_2 = recode(as.numeric(water1), !!!agre_2c)) %>%
  mutate(limit_water_4 = recode(as.numeric(water2), !!!agre_4)) %>%
  mutate(limit_water_2 = recode(as.numeric(water2), !!!agre_2)) %>%
  mutate(tax_water_4 = recode(as.numeric(water3), !!!agre_4)) %>%
  mutate(tax_water_2 = recode(as.numeric(water3), !!!agre_2)) %>%
  mutate(reduce_water_4 = recode(as.numeric(water5), !!!agre_4)) %>%
  mutate(reduce_water_2 = recode(as.numeric(water5), !!!agre_2)) %>%
  # All these items are scored in the conservative direction
  mutate(background_guns_4 = recode(as.numeric(gun3), !!!agre_4c)) %>%
  mutate(background_guns_2 = recode(as.numeric(gun3), !!!agre_2c)) %>%
  mutate(registry_guns_4 = recode(as.numeric(gun2), !!!agre_4c)) %>%
  mutate(registry_guns_2 = recode(as.numeric(gun2), !!!agre_2c)) %>%
  mutate(age_guns_4 = recode(as.numeric(gun4), !!!agre_4c)) %>%
  mutate(age_guns_2 = recode(as.numeric(gun4), !!!agre_2c)) %>%
  mutate(assault_6 = recode(as.numeric(gun5), `1` = 1, `2` = 2, `3` = 3, `4` = 4, `5` = 5, `6` = 6)) %>%
  mutate(assault_2 = recode(as.numeric(gun5), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1, `6` = 1)) %>%
  mutate(abortion_4 = recode(as.numeric(abortion2), `1` = 1, `2` = 2, `3` = 3, `4` = 4)) %>%
  mutate(abortion_2 = recode(as.numeric(abortion2), `1` = 0, `2` = 0, `3` = 0, `4` = 1)) %>%
  mutate(abortion_jail_4 = recode(as.numeric(abortion1), `1` = 4, `2` = 3, `3` = 2, `4` = 1)) %>%
  mutate(abortion_jail_2 = recode(as.numeric(abortion1), `1` = 0, `2` = 0, `3` = 0, `4` = 1)) %>%
  mutate(violent_4 = recode(as.numeric(crime2), `1` = 4, `2` = 3, `3` = 2, `4` = 1)) %>%
  mutate(violent_2 = recode(as.numeric(crime2), `1` = 0, `2` = 0, `3` = 0, `4` = 1)) %>%
  mutate(violent_4 = recode(as.numeric(wall), `1` = 4, `2` = 3, `3` = 2, `4` = 1)) %>%
  mutate(violent_2 = recode(as.numeric(wall), `1` = 1, `2` = 1, `3` = 0, `4` = 0)) %>%
  mutate(border_increase_4 = recode(as.numeric(wall), `1` = 4, `2` = 3, `3` = 2, `4` = 1)) %>%
  mutate(border_increase_2 = recode(as.numeric(wall), `1` = 1, `2` = 1, `3` = 0, `4` = 0)) %>%
  ## Contestation Questions, High is contest
  mutate(attend_march_4 = recode(as.numeric(contestation1), !!!agre_4)) %>%
  mutate(attend_march_2 = recode(as.numeric(contestation1), !!!agre_2)) %>%
  mutate(criticize_4 = recode(as.numeric(contestation2), !!!agre_4)) %>%
  mutate(criticize_2 = recode(as.numeric(contestation2), !!!agre_2)) %>%
  mutate(burn_4 = recode(as.numeric(contestation3), !!!agre_4)) %>%
  mutate(burn_2 = recode(as.numeric(contestation3), !!!agre_2)) %>%
  mutate(recount_4 = recode(as.numeric(contestation4), !!!agre_4)) %>%
  mutate(recount_2 = recode(as.numeric(contestation4), !!!agre_2)) %>%
  mutate(court_4 = recode(as.numeric(contestation5), !!!agre_4)) %>%
  mutate(court_2 = recode(as.numeric(contestation5), !!!agre_2)) %>%
  mutate(certify_4 = recode(as.numeric(contestation6), !!!agre_4)) %>%
  mutate(certify_2 = recode(as.numeric(contestation6), !!!agre_2)) %>%
  mutate(concede_4 = recode(as.numeric(contestation7), !!!agre_4)) %>%
  mutate(concede_2 = recode(as.numeric(contestation7), !!!agre_2)) %>%
  mutate(legislator_4 = recode(as.numeric(contestation8), !!!agre_4)) %>%
  mutate(legislator_2 = recode(as.numeric(contestation8), !!!agre_2)) %>%
  mutate(violent_4 = recode(as.numeric(contestation9), !!!agre_4)) %>%
  mutate(violent_2 = recode(as.numeric(contestation9), !!!agre_2)) %>%
  mutate(new_election_4 = recode(as.numeric(contestation10), !!!agre_4)) %>%
  mutate(new_election_2 = recode(as.numeric(contestation10), !!!agre_2)) %>%
  mutate(stolen_2022_4 = recode(as.numeric(steal_2020), !!!agre_4)) %>%
  mutate(stolen_2022_2 = recode(as.numeric(steal_2020), !!!agre_2)) %>%
  mutate(free_fair_4 = recode(as.numeric(free_and_fair), !!!agre_4c)) %>%
  mutate(free_fair_2 = recode(as.numeric(free_and_fair), !!!agre_2c)) %>%
  # Not confident
  mutate(vote_confidence_4 = recode(as.numeric(vote_confidence), !!!agre_4c)) %>%
  mutate(vote_confidence_2 = recode(as.numeric(vote_confidence), !!!agre_2c)) %>%
  # Not COncerned about violence
  mutate(vote_confidence_4 = recode(as.numeric(vote_confidence), !!!agre_4c)) %>%
  mutate(vote_confidence_2 = recode(as.numeric(vote_confidence), !!!agre_2c)) %>%
  # Authoritarianism
  mutate(auth_1 = recode(as.numeric(auth1), `1` = 0, `2` = 1)) %>%
  mutate(auth_2 = recode(as.numeric(auth2), `1` = 0, `2` = 1)) %>%
  mutate(auth_3 = recode(as.numeric(auth3), `1` = 1, `2` = 0)) %>%
  mutate(auth_4 = recode(as.numeric(auth4), `1` = 0, `2` = 1)) %>%
  mutate(authoritarianism = rowMeans((cbind(auth_1, auth_2, auth_3, auth_4)))) %>%
  mutate(rr1 = recode(as.numeric(rr1), !!!agre_4c)) %>%
  mutate(rr2 = recode(as.numeric(rr2), !!!agre_4)) %>%
  mutate(rr3 = recode(as.numeric(rr3), !!!agre_4)) %>%
  mutate(rr4 = recode(as.numeric(rr4), !!!agre_4)) %>%
  mutate(racial_resentment = rowMeans((cbind(rr1, rr2, rr3, rr4)))) %>%
  # Media consumption, coded to almost never, rarely, occasionally, very often
  mutate(media_major = recode(as.numeric(news1), !!!agre_4)) %>%
  # Media type, always, often
  # create variables prefaced with n_ and then is fox, cnn, msnbc, local , nyt, wp, wsj, latimes, usatoday,
  # localnewspaper, npr, telmundo, facebook, twitter, gab, parler, truth, OAN, breitbart, infowars, reddit,
  # alex_jones, american_thinker, last_refuge, epoch_times, newsmax,
  mutate(n_fox = recode(as.numeric(news2_1), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_cnn = recode(as.numeric(news2_2), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_msnbc = recode(as.numeric(news2_3), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_local = recode(as.numeric(news2_4), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_nyt = recode(as.numeric(news2_5), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_wp = recode(as.numeric(news2_6), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_wsj = recode(as.numeric(news2_7), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_latimes = recode(as.numeric(news2_8), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_usatoday = recode(as.numeric(news2_9), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_localnewspaper = recode(as.numeric(news2_10), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_npr = recode(as.numeric(news2_11), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_telmundo = recode(as.numeric(news2_12), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_facebook = recode(as.numeric(news2_13), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_twitter = recode(as.numeric(news2_14), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_gab = recode(as.numeric(news2_15), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_parler = recode(as.numeric(news2_16), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_truth = recode(as.numeric(news2_17), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_oan = recode(as.numeric(news2_18), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_breitbart = recode(as.numeric(news2_19), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_infowars = recode(as.numeric(news2_20), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_reddit = recode(as.numeric(news2_21), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_alex_jones = recode(as.numeric(news2_22), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_american_thinker = recode(as.numeric(news2_23), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_last_refuge = recode(as.numeric(news2_24), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_epoch_times = recode(as.numeric(news2_25), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_newsmax = recode(as.numeric(news2_26), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_daily_wire = recode(as.numeric(news2_27), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  # continue with variable names: n_ + censored_news, redstate, townhall, steven_crowder, washington_examiner, drudge,
  mutate(n_censored_news = recode(as.numeric(news2_28), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_redstate = recode(as.numeric(news2_29), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_townhall = recode(as.numeric(news2_30), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_steven_crowder = recode(as.numeric(news2_31), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_washington_examiner = recode(as.numeric(news2_32), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(n_drudge = recode(as.numeric(news2_33), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 1)) %>%
  mutate(media_trust_4 = recode(as.numeric(news3), !!!agre_4)) %>%
  mutate(media_trust_2 = recode(as.numeric(news3), !!!agre_2)) %>%
  # Demographics
  mutate(az_10years = recode(as.numeric(az_res), `1` = 0, `2` = 0, `3` = 0, `4` = 1)) %>%
  mutate(interest_in_politics = recode(as.numeric(interest), `1` = 1, `2` = 1, `3` = 0, `4` = 0)) %>%
  mutate(age = 2022 - as.numeric(birthyr)) %>%
  # 1 = white, 2 = black, 3 = hispanic, 4 = asian, 5 = native, 6 = two_or_more, 7=other, 8 = middle eastern,
  # mutate(racial_group = recode(as.numeric(race), `1` = "White", `2` = "Black", `3` = "Hispanic", `4` = "Asian", `5` = "Native", `6` = "Other", `7` = "Other", `8` = "Middle Eastern")) %>%
  mutate(black = recode(as.numeric(race), `1` = 0, `2` = 1, `3` = 0, `4` = 0, `5` = 0, `6` = 0, `7` = 0, `8` = 0)) %>%
  mutate(white = recode(as.numeric(race), `1` = 1, `2` = 0, `3` = 0, `4` = 0, `5` = 0, `6` = 0, `7` = 0, `8` = 0)) %>%
  mutate(hispanic = recode(as.numeric(race), `1` = 0, `2` = 1, `3` = 1, `4` = 0, `5` = 0, `6` = 0, `7` = 0, `8` = 0)) %>%
  mutate(asian = recode(as.numeric(race), `1` = 0, `2` = 0, `3` = 0, `4` = 1, `5` = 0, `6` = 0, `7` = 0, `8` = 0)) %>%
  mutate(american_indian = recode(as.numeric(race), `1` = 0, `2` = 0, `3` = 0, `4` = 0, `5` = 1, `6` = 0, `7` = 0, `8` = 0)) %>%
  mutate(other = recode(as.numeric(race), `1` = 0, `2` = 1, `3` = 0, `4` = 0, `5` = 1, `6` = 1, `7` = 1, `8` = 1)) %>%
  mutate(zipcode = as.numeric(inputzip)) %>%
  mutate(married = recode(as.numeric(marstat), `1` = 1, `2` = 0, `3` = 0, `4` = 0, `5` = 0)) %>%
  mutate(female = recode(as.numeric(gender), `1` = 0, `2` = 1)) %>%
  mutate(college = recode(as.numeric(educ), `1` = 0, `2` = 0, `3` = 0, `4` = 0, `5` = 1, `6` = 1)) %>%
  # Faminc is greater than 70k
  mutate(faminc = ifelse(as.numeric(faminc_new) > 8, 1, 0)) %>%
  mutate(kids_in_home = ifelse(as.numeric(child18) == 1, 1, 0)) %>%
  mutate(pid3 = recode(as.numeric(pid3), `1` = 1, `2` = 3, `3` = 2)) %>%
  mutate(vote_in_2016 = recode(as.numeric(presvote16post), `1` = 0, `2` = 1)) %>%
  mutate(fips_county = county_AZ) %>%
  mutate(congressional_district = cd) %>%
  mutate(legislative_district = LD_upper) %>%
  mutate(ideology = recode(as.numeric(ideo5), `1` = 1, `2` = 2, `3` = 3, `4` = 4, `5` = 5)) %>%
  # 1 = Protestant, 2 = Catholic, 3 = Mormon, 4 = Other, 5 = Jewish, 6 = Other, 7 = Other, 8 = Other, 9 = Atheist, 10 = Agnostic, 11 = Nothing, 12 = Else
  mutate(religion = recode(as.numeric(religpew), `1` = 1, `2` = 2, `3` = 3, `4` = 4, `5` = 5, `6` = 4, `7` = 4, `8` = 4, `9` = 5, `10` = 6, `11` = 7, `12` = 4)) %>%
  mutate(county_weight = weight) %>%
  mutate(county = county_AZ) %>%
  mutate(LD = legislative_district) %>%
  mutate(CD = congressional_district) %>%
  mutate(Party_Selector = pid3) %>%
  select(age:Party_Selector) %>%
  select(!ends_with("_4"))


df <- df %>%
  mutate(rid = seq(1:nrow(df))) %>%
  mutate_if(is.numeric, as.character) %>%
  pivot_longer(cols = -c(rid, county_weight, county, CD, LD, Party_Selector))

df <- df %>%
  mutate(county_name = recode(
    county,
    `1` = "Apache",
    `3` = "Cochise",
    `5` = "Coconino",
    `7` = "Gila",
    `9` = "Graham",
    `11` = "Greenlee",
    `12` = "La Paz",
    `13` = "Maricopa",
    `15` = "Mohave",
    `17` = "Navajo",
    `19` = "Pima",
    `21` = "Pinal",
    `23` = "Santa Cruz",
    `25` = "Yavapai",
    `27` = "Yuma"
  ))

df <- df %>% na.omit()
labels <- read.csv("/Users/Chris/Dropbox/github_repos/the_az_voter_project/the_az_voter_project/ML/data/labels.csv")
dat <- merge(df, labels, by.x = "name", by.y = "name", all.x = TRUE)
write.csv(dat, "/Users/Chris/Dropbox/github_repos/the_az_voter_project/the_az_voter_project/ML/data/bq_upload_.csv", row.names = FALSE)
```

Upload to GCP then create a table called **PublicOpinion_Fall2022**. Then, here are the SQL queries to create a file called **az-voter-file.AVPv2.public_opinion_actionable**.

```{sql}
  -- Four Tables
  -- (3) Aggregates from voter file, CD, LD, County
  -- (1) Public Opinion data,
CREATE OR REPLACE TABLE `az-voter-file.AVPv2.poLD` as(
WITH
  aggregates AS (
  SELECT
    LD,
    SUM(Republican) as RepublicanLD,
    SUM(Democrat)   as DemocratLD,
    SUM(Independent)   as IndependentLD,
    AVG(respondent_age) AS registrant_ageLD,
    COUNT(*) as number_votersLD
  FROM
    `az-voter-file.AVPv2.datReduced`
  GROUP BY
    LD )
SELECT
  aggregates.*,
  SAFE_DIVIDE(aggregates.RepublicanLD, aggregates.number_votersLD) AS republican_percentageLD,
  SAFE_DIVIDE(aggregates.DemocratLD, aggregates.number_votersLD)    AS democrat_percentageLD,
  SAFE_DIVIDE(aggregates.IndependentLD, aggregates.number_votersLD) AS independent_percentageLD,
FROM
  aggregates
  WHERE LD IS NOT NULL
);

CREATE OR REPLACE TABLE `az-voter-file.AVPv2.poCD` as(
WITH
  aggregates AS (
  SELECT
    CD,
    SUM(Republican) as RepublicanCD,
    SUM(Democrat)   as DemocratCD,
    SUM(Independent)   as IndependentCD,
    AVG(respondent_age) AS registrant_ageCD,
    COUNT(*) AS number_votersCD
  FROM
      `az-voter-file.AVPv2.datReduced`  GROUP BY
    CD )
SELECT
  aggregates.*,
  SAFE_DIVIDE(aggregates.RepublicanCD, aggregates.number_votersCD) AS republican_percentageCD,
  SAFE_DIVIDE(aggregates.DemocratCD, aggregates.number_votersCD) AS democrat_percentageCD,
  SAFE_DIVIDE(aggregates.IndependentCD, aggregates.number_votersCD) AS independent_percentageCD,
FROM
  aggregates
  WHERE CD IS NOT NULL
);

CREATE OR REPLACE TABLE `az-voter-file.AVPv2.poCounty` as(
WITH
  aggregates AS (
  SELECT
    County,
    SUM(Republican) as RepublicanCounty,
    SUM(Democrat)   as DemocratCounty,
    SUM(Independent)   as IndependentCounty,
    AVG(respondent_age) AS registrant_ageCounty,
    COUNT(*) AS number_votersCounty
  FROM
      `az-voter-file.AVPv2.datReduced`  GROUP BY
    County )
SELECT
  aggregates.*,
  SAFE_DIVIDE(aggregates.RepublicanCounty, aggregates.number_votersCounty) AS republican_percentageCounty,
  SAFE_DIVIDE(aggregates.DemocratCounty, aggregates.number_votersCounty) AS democrat_percentageCounty,
  SAFE_DIVIDE(aggregates.IndependentCounty, aggregates.number_votersCounty) AS independent_percentageCounty,
FROM
  aggregates
  WHERE County IS NOT NULL
);


CREATE OR REPLACE TABLE `az-voter-file.AVPv2.public_opinion_actionable` as(
with county as (SELECT
t1.*,
t2.* except(County)
  FROM `az-voter-file.AVPv2.PublicOpinion_Fall2022` as t1 JOIN  `az-voter-file.AVPv2.poCounty` as t2 ON
  t1.county_name = t2.County
)
SELECT
t1.*,
t2.* except(LD)
  FROM county as t1
  JOIN  `az-voter-file.AVPv2.poLD` as t2 ON
  cast(t1.LD as string) = t2.LD
  JOIN  `az-voter-file.AVPv2.poCD` as t3 ON
  cast(t1.CD as string) = t3.CD
);

SELECt
* except(county)
FROM `az-voter-file.AVPv2.public_opinion_actionable`
```

## Precincts

```{sql}
CREATE OR REPLACE TABLE `az-voter-file.AVPv2.precinctsGold` AS (
 WITH t1 AS (
  SELECT
    Lean_Democrat,
    Lean_Republican,
    Defect_to_Republican,
    latitudeRecent AS latitude,
    longitudeRecent AS longitude,
    party_identification,
    CASE
      WHEN GENERAL_e2022 = 1 AND PRIMARY_e2022 = 1 AND PRIMARY_e2020 = 1 AND GENERAL_e2020 = 1 THEN 1
    ELSE
    0
  END
    AS twoXtwo,
    GENERAL_e2022,
    PRIMARY_e2022,
    GENERAL_e2020,
    PRIMARY_e2020,
    GENERAL_e2018,
    PRIMARY_e2018,
    GENERAL_e2016,
    PRIMARY_e2016,
    Republican,
    Democrat,
    Independent,
    ST_GEOGPOINT(longitudeRecent, latitudeRecent) AS location
  FROM
    `az-voter-file.AVPv2.datReduced`),
  # This is the precinct file, built from the state's xml file.
  geo AS(
  SELECT
    geo.precinct,
    geo.geo,
  FROM
    `az-voter-file.AVPv2.precincts_actionable` AS geo
  WHERE
    ballot_candidate = "Hobbs_Katie" ),
  voter_file AS(
  SELECT
    SUM(Republican) AS Republicans,
    SUM(Democrat) AS Democrats,
    SUM(Independent) AS Independents,
    SUM(twoXtwo) AS two_two,
    geo2.precinct
  FROM
    t1 AS t1
  CROSS JOIN
    geo AS geo2
  WHERE
    ST_CONTAINS(SAFE.st_geogfromgeojson(geo2.geo),location)
  GROUP BY
    precinct ),
  voter_file_and_ld AS (
  SELECT
    voter_file.*,
    precincts.* except(precinct),
    ST_CENTROID(SAFE.st_geogfromgeojson(precincts.geo)) AS point,
    ST_X(ST_CENTROID(SAFE.st_geogfromgeojson(precincts.geo))) AS longitude,
    ST_Y(ST_CENTROID(SAFE.st_geogfromgeojson(precincts.geo))) AS latitude,
  FROM
    voter_file AS voter_file
  JOIN
    `az-voter-file.AVPv2.precincts_actionable` AS precincts
  ON
    voter_file.precinct = precincts.precinct ),
  voter_file_with_cd AS(
  SELECT
    voter_file_and_ld.*,
    geo.cd AS legislative_district
  FROM
    voter_file_and_ld
  CROSS JOIN
    `az-voter-file.AVPv2.ld_geo` AS geo
  WHERE
    ST_CONTAINS(ST_GEOGFROMGEOJSON(geo.geo),voter_file_and_ld.point) ),
  cds AS (
  SELECT
    voter_file_with_cd.*,
    geo.cd AS congressional_district
  FROM
    voter_file_with_cd AS voter_file_with_cd
  CROSS JOIN
    `az-voter-file.AVPv2.geo_cd` AS geo
  WHERE
    ST_CONTAINS(ST_GEOGFROMGEOJSON(geo.geo),voter_file_with_cd.point) ),
  blocks AS (
  SELECT
    cds.*,
    bg.geo_ID AS geographic_identifier,
    bg.internal_point_lat AS block_latitude,
    bg.internal_point_lon AS block_longitude
  FROM
    cds
  CROSS JOIN
    `bigquery-public-data.geo_census_blockgroups.blockgroups_04` AS bg
  WHERE
    ST_CONTAINS(bg.blockgroup_geom, point) )
SELECT
  *,
FROM
  blocks
LEFT JOIN
  `bigquery-public-data.census_bureau_acs.blockgroup_2018_5yr` AS a
ON
  blocks.geographic_identifier = a.geo_id
  )
  --   SELECT
  -- *,
  -- safe_subtract(cast(Yee_Kimberly as int), cast(Masters_Blake as int)) as Yee_Margin,
  -- safe_add(cast(Fontes_Adrian as int), cast(Finchem_Mark as int)) as SoS_Total,
  -- safe_subtract(cast(Mayes_Kris as int), cast(Hamadeh_Abraham_Abe as int)) as Mayes_Margin,
  -- safe_add(cast(Mayes_Kris as int), cast(Hamadeh_Abraham_Abe as int)) as AG_Total,
  -- safe_subtract(cast(Quezada_Martin as int), cast(Yee_Kimberly as int)) as Quezada_margin,
  -- safe_add(cast(Quezada_Martin as int), cast(Yee_Kimberly as int)) as Treasurer_Total,
  -- safe_subtract(cast(Kelly_Mark as int), cast(Masters_Blake as int)) as Kelly_Margin,
  -- safe_add(cast(Kelly_Mark as int), cast(Masters_Blake as int)) as Senate_Total,
  -- safe_subtract(cast(Hobbs_Katie as int), cast(Lake_Kari as int)) as Hobbs_Margin,
  -- safe_add(cast(Hobbs_Katie as int), cast(Lake_Kari as int)) as Governor_Total,
  -- FROM `az-voter-file.public_data.precinct_counts_07`
```
