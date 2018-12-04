
#Package Calls

library(RPostgreSQL)
library(data.table)
library(dplyr)

################################################
## CREATE DATABASE CONNECTION
#################################################

drv <- RPostgreSQL::PostgreSQL()

con <- DBI::dbConnect( drv, dbname = 'PNM_CH_SA', user = 'postgres',
                      host = '127.0.0.1', port = 5432, password = 'postgres')

################################################
## EXTRACT REFERENCE DATA
#################################################

#Reference Data - Contains all probelamtic nodes which are confirmed CPD issue

referenceTest <-
  DBI::dbGetQuery(con, "SELECT * FROM TEST_CASES.test_cases_reference_data_connected_mac_address;")


################################################
## EXTRACT POLLING DATA
#################################################

# CPE Data set - Filter out all CPEs data for all probelamtic nodes from polling dataset

CPEData <-
  DBI::dbGetQuery(con,"SELECT DISTINCT a.node_name, a.hashed_src_node_id,
                  a.hour_stamp, a.snr_up, a.snr_dn, a.pathloss,
                  a.txpower_up, a.rxpower_up  FROM 
                  TEST_CASES.test_cases_polling_data_all a,
                  TEST_CASES.test_cases_reference_data_connected_mac_address b
                  where a.topo_node_type = 'CPE' and a.node_name = b.node_name
                  ORDER BY a.node_name, a.hashed_src_node_id, a.hour_stamp; ")


################################################
## COLUMN NAME CHANGES
#################################################

# Change the hashed src node id column name

setnames(referenceTest, "hashed_connected_mac_addresses", "mac_addresses")
setnames(CPEData, "hashed_src_node_id", "src_node_id")

################################################
## CLOSING THE DATABASE CONNECTION
#################################################

# Close the posgres DB connection

on.exit(dbDisconnect(con))

# #################################################
# ## SAVE R SESSION
# #################################################

save.image(file='Extract_Data_Sources.RData')

# To load this R session:

# load('Extract_Data_Sources.RData')





