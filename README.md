# The Arizona Voter Project
##

#BQ Queries
THe big query queries used to construct the data on the website are located in the bigquery folder. They're also saved to the shared queries in Big Query. Here's a brief description

(1) masterAddresses: Constructs a table of unique registrants and addresses

(2) fullGeo: Joins voter data with geocoded data. Data geocoded in ArcGIS

(3) oldVote: Joins the most recent build of the data with an older version, which includes older records

(4) geoBlocks: Add block level information

(5) preML: Construct the data used in R/Python/Tensorflow to construct latent engagement scores and detail 
their accuracy.

(6) These files are used to create a table in BQ called fullPostML

(7) datReduced: Create a smaller version of the data

(8) tableauDat: Organize data for tableau

(9) actionDat: construct an actionable version of the table, following a star schema.

(10-13) census(LDCD; County; Blocks): Construct census, county aggregates.

14) blockCorrection: Identify blocks in CD/LD

<html/>
<iframe src="https://slides.com/christopherweber/freedom-center/embed" width="576" height="420" title="Freedom Center Presentation" scrolling="no" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen></iframe>
</html>
