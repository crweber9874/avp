-- Create county aggregates 

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