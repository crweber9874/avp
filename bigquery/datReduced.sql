   -- - Post ML Upload --
   -- This generates a reduced version of the data, suitable for loading into tableau
   -- The end result is a subset of census items, full voter file, that amounts to 
   -- 4194440 "active" records
   CREATE OR REPLACE VIEW
     `az-voter-file.AVPv2.data_merged` AS(
      with tableA as (
    SELECT
       *
     FROM  `az-voter-file.AVPv2.fullPostML`
  ),
  tableB as(
    SELECT
  *
  FROM  `az-voter-file.AVPv2.dataForML`
  )
  SELECT
  tableA.*,
  tableB.* except(Registrant_ID),
  FROM tableA  LEFT JOIN tableB ON
   CAST(tableA.Registrant_ID AS int) = CAST(tableB.registrant_id AS int)
   );

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

SELECT
*
FROM  `az-voter-file.AVPv2.datReduced`

