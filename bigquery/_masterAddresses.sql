  # Create two tables, 
  # masterRegistrants: a table of registrant ID and Address
  # masterAddresses:   a table of unique addresses
  CREATE OR REPLACE VIEW
    `az-voter-file.AVPv2.masterRegistrants` AS(
    SELECT
      registrant_id,
      (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' || Residence_State || ', ' || Residence_Zip) AS addressInVote,
    FROM
      `az-voter-file.AVPv2.combined_1104` );

  CREATE OR REPLACE VIEW
    `az-voter-file.AVPv2.masterAddresses`AS(
    SELECT
      DISTINCT(addressInVote),
    FROM
      `az-voter-file.AVPv2.masterRegistrants` );
   CREATE OR REPLACE VIEW
    `az-voter-file.AVPv2.masterAddresses`AS(
    SELECT
      DISTINCT(addressInVote),
    FROM
      `az-voter-file.AVPv2.masterRegistrants` );
 