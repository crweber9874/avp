# Merge to older records, if available. 
# Describe this 
-- Create a description in the table called, "old data"
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.oldVote`AS(
  SELECT
    Registrant_ID AS registrantID,
    __08_2016_2016_GENERAL_ELECTION AS general_2016,
    __06_2018_2018_GENERAL_ELECTION AS general_2018,
    __28_2018_2018_PRIMARY_ELECTION AS primary_2018,
    __30_2016_2016_PRIMARY_ELECTION AS priamry_2016,
  FROM
   `az-voter-file.AVPv2.combined_1104`
  WHERE
    BUILD_DATE = '2022-01-25'
    AND (Status = "Active" OR Status = "ACTIVE"));
  -- Combine the two, this is called voteDataActionable.,
CREATE OR REPLACE VIEW
  `az-voter-file.AVPv2.voteDataActionable`AS(
  SELECT
    a.* EXCEPT(registrantID),
    b.*
  FROM
    `az-voter-file.AVPv2.oldVote` AS a
  RIGHT JOIN
    `az-voter-file.AVPv2.fullDataGeo` AS b
  ON
    a. registrantID = b.registrantID );
