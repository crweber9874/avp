# Create geocoded table
# Joins the data, built 07-03-24 with the geocoded data 
CREATE OR REPLACE VIEW
    `az-voter-file.AVPv2.fullData`AS(
    SELECT
      *,
      (Residence_Address || ', ' || Residence_City || ', ' || County|| ', ' || Residence_State || ', ' || Residence_Zip) AS addressInVote,
      Registrant_ID AS registrantID
    FROM
      `az-voter-file.AVPv2.combined_1104`
    WHERE 
      BUILD_DATE = "2023-07-03"
      AND (Status = "Active" OR Status = "ACTIVE")

      );
    CREATE OR REPLACE VIEW
    `az-voter-file.AVPv2.fullDataGeo`AS(
    SELECT
      SAFE_CAST(Score1 AS FLOAT64) AS scores,
      a.Match_type1 AS matchType,
      SAFE_CAST(X1 AS float64) AS longitude,
      SAFE_CAST(Y1 AS float64) AS latitude,
      IN_Address1,
      b.*,
    FROM
      `az-voter-file.AVPv2.geocoded_1104` AS a
    RIGHT JOIN
      `az-voter-file.AVPv2.fullData` AS b
    ON
      a.In_Address1 = b.addressInVote
   );