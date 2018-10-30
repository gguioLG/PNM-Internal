################# Setting up the Postgres DB connection ################

#Use local library
.libPaths("/app/RPackageLibrary/R-3.3")

#Package Calls
library(RPostgreSQL)
library(imputeTS)
library(dplyr)
library(data.table)
library(plyr)
library(ggplot2)
library(reshape2)
library(TSDist)

#Create DB connection to localhost

drv <- RPostgreSQL::PostgreSQL()
con <-  DBI::dbConnect(drv,
    dbname = 'PNM_CH_SA',
    user = 'postgres',
    host = '127.0.0.1',
    port = 5432,
    password = 'postgres'
  )

#Reference Data - Contains all probelamtic nodes which are confirmed CPD issue

referenceTest <-
  DBI::dbGetQuery(con, "SELECT * FROM TEST_CASES.TEST_CASES_REFERENCE_DATA;")

#Node Data set - Filter out the time series data from polling dataset for all probelamtic nodes. The snr_up for each node is calculated by averaging snr_up for each CPE under a node.

NodeData <-
  DBI::dbGetQuery(
    con,
    "SELECT DISTINCT a.node_name, a.hour_stamp, AVG(a.snr_up) as snr_up FROM TEST_CASES.test_cases_polling_data a, TEST_CASES.TEST_CASES_REFERENCE_DATA b where a.topo_node_type = 'CPE' and a.node_name = b.node_name GROUP by a.node_name, a.hour_stamp ORDER by a.node_name, a.hour_stamp; "
  )

# CPE Data set - Filter out all CPEs data for all probelamtic nodes from polling dataset

CPEData <-
  DBI::dbGetQuery(
    con,
    "SELECT DISTINCT a.* FROM TEST_CASES.test_cases_polling_data a, TEST_CASES.TEST_CASES_REFERENCE_DATA b where a.topo_node_type = 'CPE' and a.node_name = b.node_name ORDER BY a.node_name, a.hour_stamp; "
  )

# Close the posgres DB connection

on.exit(dbDisconnect(con))

# Removing duplicate rows from CPE Data. We need to remove
# the reference_date column which is causing the duplicates.

CPEData_n <- unique(CPEData[,-1])

# Creating the Mac series (src_node_id) for snr_dn

mac_snrdn <-  CPEData_n[, c("node_name", "src_node_id", "hour_stamp", "snr_dn")]
mac_snrdn <-  subset(mac_snrdn, src_node_id %in% mac_feature_list_updated$src_node_id)

# Creating the Mac series (src_node_id) for pathloss

mac_pathloss <- CPEData_n[, c("node_name", "src_node_id", "hour_stamp", "pathloss")]
mac_pathloss <- subset(mac_pathloss, src_node_id %in% mac_feature_list_updated$src_node_id)

# Imputing the NA values in the Mac snrdn dataset with Simple moving averages

mac_snrdn <-  mac_snrdn[order(mac_snrdn$node_name,
                              mac_snrdn$src_node_id,
                              mac_snrdn$hour_stamp), ]

mac_snrdn <- mac_snrdn %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_sma = na.ma(snr_dn, k = 4, weighting = "simple"))

# Imputing the NA values in the Mac pathloss dataset with Simple moving averages

mac_pathloss <-
  mac_pathloss[order(mac_pathloss$node_name,
                     mac_pathloss$src_node_id,
                     mac_pathloss$hour_stamp), ]

mac_pathloss <- mac_pathloss %>%
  group_by(src_node_id) %>%
  mutate(pathloss_sma = na.ma(pathloss, k = 4, weighting = "simple"))

# Creating a unique list of nodes and mac addresses

node_mac <- unique(CPEData_n[, c("node_name", "src_node_id")])
node_mac <-  subset(node_mac, src_node_id %in% mac_feature_list_updated$src_node_id)

# Shape-based distances - Dynamic Time Warping (DTW) Distance

for (i in 1:nrow(node_mac))
{
  
  node_series <- NA
  mac_snrdn_series  <- NA
  mac_pathloss_series  <- NA
  
  node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
  mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
  mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
  
  node_mac$snrdn_dtw[i] <- TSDistances(node_series, mac_snrdn_series , distance="dtw", sigma=10)
  node_mac$pathloss_dtw[i] <- TSDistances(node_series, mac_pathloss_series , distance="dtw",sigma=10)
}


# Edit based distances - Edit Distance for Real Sequences (EDR)

for (i in 1:nrow(node_mac))
{
  
  node_series <- NA
  mac_snrdn_series  <- NA
  mac_pathloss_series  <- NA
  
  node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
  mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
  mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
  
  node_mac$snrdn_edr[i] <- TSDistances(node_series, mac_snrdn_series , distance="edr", epsilon=0.1)
  node_mac$pathloss_edr[i] <- TSDistances(node_series, mac_pathloss_series , distance="edr", epsilon=0.1)
}

# Edit based distances - Longest Common Subsequence (LCSS) 

for (i in 1:nrow(node_mac))
{
  
  node_series <- NA
  mac_snrdn_series  <- NA
  mac_pathloss_series  <- NA
  
  node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
  mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
  mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
  
  node_mac$snrdn_lcss[i] <- TSDistances(node_series, mac_snrdn_series , distance="lcss", epsilon=0.1)
  node_mac$pathloss_lcss[i] <- TSDistances(node_series, mac_pathloss_series , distance="lcss", epsilon=0.1)
}

# Edit based distances - Edit Distance with Real Penalty (ERP) 

for (i in 1:nrow(node_mac))
{
  
  node_series <- NA
  mac_snrdn_series  <- NA
  mac_pathloss_series  <- NA
  
  node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
  mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
  mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
  
  node_mac$snrdn_erp[i] <- TSDistances(node_series, mac_snrdn_series , distance="erp", g=0)
  node_mac$pathloss_erp[i] <- TSDistances(node_series, mac_pathloss_series , distance="erp", g=0)
}


# Feaure-based distances - Cross-correlation based 

for (i in 1:nrow(node_mac))
{
  
  node_series <- NA
  mac_snrdn_series  <- NA
  mac_pathloss_series  <- NA
  
  node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
  mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
  mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
  
  node_mac$snrdn_ccor[i] <- TSDistances(node_series, mac_snrdn_series , distance="ccor")
  node_mac$pathloss_ccor[i] <- TSDistances(node_series, mac_pathloss_series , distance="ccor")
}

rm(node_series)
rm(mac_snrdn_series)
rm(mac_pathloss_series)

# Adding mac label to check the positive CPD Macs

node_mac$mac_label <- mac_positive$mac_label[match(node_mac$src_node_id, mac_positive$src_node_id)]
node_mac$mac_label[is.na(node_mac$mac_label)] <- 0








