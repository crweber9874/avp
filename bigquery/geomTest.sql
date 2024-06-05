# Creates two tables -- does precinct intersect with CD/LD (2020-2023 Geo)
# pcd: precincts/cd
# pld: precincts/ld
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.pcd` AS (
  WITH
    precinct AS (
    SELECT
      * EXCEPT(geo),
      SAFE.ST_GEOGFROMGEOJSON(geo) AS geo
    FROM
      `az-voter-file.shapes.geoPrecincts`
    WHERE
      counter IS NOT NULL
      AND precinctName != "id" ),
    cds AS (
    SELECT
      counter,
      id AS CD,
      SAFE.ST_GEOGFROMGEOJSON(geo) AS geo
    FROM
      `az-voter-file.shapes.geoCD`
    WHERE
      counter IS NOT NULL
      AND id != "id" )
  SELECT
    cds.CD,
    precinct.precinctName,
  FROM
    cds
  JOIN
    precinct
  ON
    ST_INTERSECTS(cds.geo, precinct.geo ) );
CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.pld` AS (
  WITH
    precinct AS (
    SELECT
      * EXCEPT(geo),
      SAFE.ST_GEOGFROMGEOJSON(geo) AS geo
    FROM
      `az-voter-file.shapes.geoPrecincts`
    WHERE
      counter IS NOT NULL
      AND precinctName != "id" ),
    lds AS (
    SELECT
      counter,
      LD,
      SAFE.ST_GEOGFROMGEOJSON(geo) AS geo
    FROM
      `az-voter-file.shapes.geoLDs`
    WHERE
      counter IS NOT NULL
      AND LD != "id" )
  SELECT
    lds.LD,
    precinct.precinctName,
  FROM
    lds
  JOIN
    precinct
  ON
    ST_INTERSECTS(lds.geo, precinct.geo ) );