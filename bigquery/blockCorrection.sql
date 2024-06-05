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