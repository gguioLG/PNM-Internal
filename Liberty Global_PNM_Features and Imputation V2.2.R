
#Use local library
.libPaths("/app/RPackageLibrary/R-3.3")

#Package Calls
library(RPostgreSQL)
library(imputeTS)
library(dplyr)
library(data.table)
library(plyr)
library(stringr)

#Create DB connection to localhost
drv <- RPostgreSQL::PostgreSQL()
con <-
  DBI::dbConnect(
    drv,
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

#################### Creating the feature list #####################

#Length of Mac series

mac_len <- ddply(CPEData_n, .(src_node_id), nrow)
setnames(mac_len, "V1", "mac_len")

# Missing Values for snr_dn and pathloss

mac_snrdn_na <-
  aggregate(snr_dn ~ src_node_id, data = CPEData_n, function(x) {
    sum(is.na(x))
  }, na.action = NULL)
setnames(mac_snrdn_na, "snr_dn", "snr_dn_na")

mac_pathloss_na <-
  aggregate(pathloss ~ src_node_id, data = CPEData_n, function(x) {
    sum(is.na(x))
  }, na.action = NULL)
setnames(mac_pathloss_na, "pathloss", "pathloss_na")

# Descriptive Stats for snr_dn and pathloss

mac_stats <-
  setDT(CPEData_n)[, list(
    snrdn_min = min(snr_dn, na.rm = T),
    snrdn_max = max(snr_dn, na.rm = T),
    snrdn_mean = mean(snr_dn, na.rm = T),
    snrdn_median = median(snr_dn, na.rm = T),
    snrdn_sd = sd(snr_dn, na.rm = T),
    pathloss_min = min(pathloss, na.rm = T),
    pathloss_max = max(pathloss, na.rm = T),
    pathloss_mean = mean(pathloss, na.rm = T),
    pathloss_median = median(pathloss, na.rm = T),
    pathloss_sd = sd(pathloss, na.rm = T)
  ), by = .(src_node_id)]

# Average difference between successive time stamp for non na snr dn values -

mac_snrdn_tdiff <-  CPEData_n %>%
  select(src_node_id, hour_stamp, snr_dn)

mac_snrdn_tdiff <- mac_snrdn_tdiff[!is.na(mac_snrdn_tdiff$snr_dn), ]

mac_snrdn_tdiff <-
  mac_snrdn_tdiff[order(mac_snrdn_tdiff$src_node_id, mac_snrdn_tdiff$hour_stamp), ]

mac_snrdn_tdiff$snrdn_tdiff <-
  unlist(
    tapply(
      mac_snrdn_tdiff$hour_stamp,
      INDEX = mac_snrdn_tdiff$src_node_id,
      FUN = function(x)
        c(0, diff(as.numeric(x)))
    )
  )

mac_snrdn_tdiff <-
  aggregate(snrdn_tdiff ~ src_node_id, data = mac_snrdn_tdiff, function(x) {
    mean(x)
  }, na.action = NULL)

# Average difference between successive time stamp for non na pathloss values -

mac_pathloss_tdiff <-  CPEData_n %>%
  select(src_node_id, hour_stamp, pathloss)

mac_pathloss_tdiff <-
  mac_pathloss_tdiff[!is.na(mac_pathloss_tdiff$pathloss), ]

mac_pathloss_tdiff <-
  mac_pathloss_tdiff[order(mac_pathloss_tdiff$src_node_id,
                           mac_pathloss_tdiff$hour_stamp), ]

mac_pathloss_tdiff$pathloss_tdiff <-
  unlist(
    tapply(
      mac_pathloss_tdiff$hour_stamp,
      INDEX = mac_pathloss_tdiff$src_node_id,
      FUN = function(x)
        c(0, diff(as.numeric(x)))
    )
  )

mac_pathloss_tdiff <-
  aggregate(pathloss_tdiff ~ src_node_id, data = mac_pathloss_tdiff, function(x) {
    mean(x)
  }, na.action = NULL)

# Average difference between successive non na snr_dn values -

mac_snrdn_delta <-  CPEData_n %>%
  select(src_node_id, hour_stamp, snr_dn)

mac_snrdn_delta <- mac_snrdn_delta[!is.na(mac_snrdn_delta$snr_dn), ]

mac_snrdn_delta <-
  mac_snrdn_delta[order(mac_snrdn_delta$src_node_id, mac_snrdn_delta$hour_stamp), ]

mac_snrdn_delta$snrdn_delta <-
  unlist(
    tapply(
      mac_snrdn_delta$snr_dn,
      INDEX = mac_snrdn_delta$src_node_id,
      FUN = function(x)
        c(0, diff(as.numeric(x)))
    )
  )

mac_snrdn_delta <-
  aggregate(snrdn_delta ~ src_node_id, data = mac_snrdn_delta, function(x) {
    mean(x)
  }, na.action = NULL)

# Average difference between successive non na pathloss values -

mac_pathloss_delta <-  CPEData_n %>%
  select(src_node_id, hour_stamp, pathloss)

mac_pathloss_delta <-
  mac_pathloss_delta[!is.na(mac_pathloss_delta$pathloss), ]

mac_pathloss_delta <-
  mac_pathloss_delta[order(mac_pathloss_delta$src_node_id,
                           mac_pathloss_delta$hour_stamp), ]

mac_pathloss_delta$pathloss_delta <-
  unlist(
    tapply(
      mac_pathloss_delta$pathloss,
      INDEX = mac_pathloss_delta$src_node_id,
      FUN = function(x)
        c(0, diff(as.numeric(x)))
    )
  )

mac_pathloss_delta <-
  aggregate(pathloss_delta ~ src_node_id, data = mac_pathloss_delta, function(x) {
    mean(x)
  }, na.action = NULL)


# Ratio of snr_dn to snr_up for each mac

mac_snr_ratio <- CPEData_n %>%
  select(src_node_id, hour_stamp, snr_dn, snr_up)

mac_snr_ratio <- mac_snr_ratio[!is.na(mac_snr_ratio$snr_dn), ]
mac_snr_ratio <- mac_snr_ratio[!is.na(mac_snr_ratio$snr_up), ]

mac_snr_ratio$snr_ratio <- mac_snr_ratio$snr_dn / mac_snr_ratio$snr_up

mac_snr_ratio <-
  aggregate(snr_ratio ~ src_node_id, data = mac_snr_ratio, function(x) {
    mean(x)
  }, na.action = NULL)

# Ratio of pathloss to txpower_up for each mac

mac_pathloss_txpwrup_ratio <- CPEData_n %>%
  select(src_node_id, hour_stamp, pathloss, tx_pwr_up)

mac_pathloss_txpwrup_ratio <-
  mac_pathloss_txpwrup_ratio[!is.na(mac_pathloss_txpwrup_ratio$pathloss), ]
mac_pathloss_txpwrup_ratio <-
  mac_pathloss_txpwrup_ratio[!is.na(mac_pathloss_txpwrup_ratio$tx_pwr_up), ]

mac_pathloss_txpwrup_ratio$pathloss_txpwrup_ratio <-
  mac_pathloss_txpwrup_ratio$pathloss / mac_pathloss_txpwrup_ratio$tx_pwr_up

mac_pathloss_txpwrup_ratio <-
  aggregate(pathloss_txpwrup_ratio ~ src_node_id, data = mac_pathloss_txpwrup_ratio, function(x) {
    mean(x)
  }, na.action = NULL)

# Ratio of pathloss to rxpower_up for each mac

mac_pathloss_rxpwrup_ratio <- CPEData_n %>%
  select(src_node_id, hour_stamp, pathloss, rx_pwr_up)

mac_pathloss_rxpwrup_ratio <-
  mac_pathloss_rxpwrup_ratio[!is.na(mac_pathloss_rxpwrup_ratio$pathloss), ]
mac_pathloss_rxpwrup_ratio <-
  mac_pathloss_rxpwrup_ratio[!is.na(mac_pathloss_rxpwrup_ratio$rx_pwr_up), ]

mac_pathloss_rxpwrup_ratio$pathloss_rxpwrup_ratio <-
  mac_pathloss_rxpwrup_ratio$pathloss / mac_pathloss_rxpwrup_ratio$rx_pwr_up

mac_pathloss_rxpwrup_ratio <-
  aggregate(pathloss_rxpwrup_ratio ~ src_node_id, data = mac_pathloss_rxpwrup_ratio, function(x) {
    mean(x)
  }, na.action = NULL)

# Positive mac adresses from reference data set

mac_positive <- as.data.frame(unlist(strsplit(referenceTest$mac_address, ",")))
colnames(mac_positive) <- "src_node_id"

mac_positive$src_node_id <- str_replace_all(mac_positive$src_node_id, ':', '')
mac_positive$src_node_id <- str_replace_all(mac_positive$src_node_id, ',', '')

mac_positive <- mac_positive[! mac_positive$src_node_id == "NA", , drop=F]
mac_positive <- mac_positive %>% distinct(src_node_id, .keep_all = TRUE)
mac_positive$mac_label <- 1

# Merging the feature list

mac_feature_list <-
  merge(x = mac_len,
        y = mac_snrdn_na,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list$perc_snrdn_na <-
  mac_feature_list$snr_dn_na / mac_feature_list$mac_len

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_pathloss_na,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list$perc_pathloss_na <-
  mac_feature_list$pathloss_na / mac_feature_list$mac_len

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_stats,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_snrdn_tdiff,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_pathloss_tdiff,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_snrdn_delta,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_pathloss_delta,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_snr_ratio,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_pathloss_txpwrup_ratio,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_pathloss_rxpwrup_ratio,
        by = "src_node_id",
        all.x = TRUE)

mac_feature_list$mac_label <- mac_positive$mac_label[match(mac_feature_list$src_node_id, mac_positive$src_node_id)]

mac_feature_list$mac_label[is.na(mac_feature_list$mac_label)] <- 0



# Removing mac adresses where percentage na values for snr_dn or pathloss is greater than 50%

mac_feature_list_updated <-
  subset(mac_feature_list , perc_snrdn_na <= 0.5)

mac_feature_list_updated <-
  subset(mac_feature_list_updated , perc_pathloss_na <= 0.5)

##################### Imputation ####################

# Imputing the NA values in the node dataset with Simple moving averages(SMA). The 
# SMA method was chosen since it was performing better than most of the other methods
# based on RMSE metric.


NodeData <- NodeData[order(NodeData$node_name, NodeData$hour_stamp), ]

NodeData <- NodeData %>%
  group_by(node_name) %>%
  mutate(imp_snr_up = na.ma(snr_up, k = 4, weighting = "simple"))

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


