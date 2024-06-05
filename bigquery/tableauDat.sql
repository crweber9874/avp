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
  LEFT JOIN
    `az-voter-file.AVPv2.centroids` AS centroids
  ON
    star.census_block = centroids.census_block );
  SELECT
  * 
  FROM `az-voter-file.AVPv2.starTableCent` 
