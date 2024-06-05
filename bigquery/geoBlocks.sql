# Creates two tables
# voteDataActionable
# Joins with 2010 Census  Block Groups.
# The pandemic delayed the ACS. Also, codes
# addresses into census blocks, GoldAddressError
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.voteDataActionablePr`AS(
  SELECT
    *,
    precinct AS GeoPrecinct
  FROM
    `az-voter-file.AVPv2.voteDataActionable` );
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.goldAddressError`AS(
  SELECT
    *,
    bg.geo_id AS census_block,
    ST_GEOGPOINT(SAFE_CAST(longitude AS float64), SAFE_CAST(latitude AS float64) ) AS location_in_address_file,
  FROM
    `az-voter-file.AVPv2.voteDataActionablePr`
  CROSS JOIN
    `bigquery-public-data.geo_census_blockgroups.blockgroups_04` AS bg
  WHERE
    ST_CONTAINS(bg.blockgroup_geom, ST_GEOGPOINT(SAFE_CAST(longitude AS float64), SAFE_CAST(latitude AS float64) )) );

