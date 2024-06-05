CREATE OR REPLACE TABLE
  `az-voter-file.AVPv2.starAggregates`AS(
SELECT
* except(centroid_longitude, centroid_latitude)
     FROM
    `az-voter-file.AVPv2.starTableCent` AS star
  LEFT JOIN
    `az-voter-file.AVPv2.cdAggregates` AS cd
  ON
    star.CD = cast(cd.congressional_district as INT)
  LEFT JOIN
    `az-voter-file.AVPv2.ldAggregates` AS ld
  ON
    star.LD  = cast(ld.legislative_district as INT)
  LEFT JOIN
    `az-voter-file.AVPv2.countyAggregates` AS county
  ON
    cast(star.County as string) = county.county_name
  LEFT JOIN
    `az-voter-file.AVPv2.blockAggregates` AS block
  ON
    block.census_block_ = star.census_block
  );

