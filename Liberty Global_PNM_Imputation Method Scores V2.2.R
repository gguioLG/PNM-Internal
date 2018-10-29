################# Setting up the Postgres DB connection ################

#Use local library
.libPaths("/app/RPackageLibrary/R-3.3")

#Package Calls
library(RPostgreSQL)
library(imputeTS)
library(dplyr)
library(data.table)
library(plyr)

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

# Imputing the avg snr up for each node from the values through different 
# methods (Interpolation, Moving Avergages etc...)

NodeData_n <- NodeData[order(NodeData$node_name, NodeData$hour_stamp), ]

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_kalman = na.kalman(snr_up))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_mean = na.mean(snr_up, option = "mean"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_median = na.mean(snr_up, option = "median"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_mode = na.mean(snr_up, option = "mode"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_random = na.random(snr_up))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_locf = na.locf(snr_up, option = "locf"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_nocb = na.locf(snr_up, option = "nocb"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_interp_linear = na.interpolation(snr_up, option = "linear"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_interp_spline = na.interpolation(snr_up, option = "spline"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_ma_expo = na.ma(snr_up, k = 4, weighting = "exponential"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_ma_linear = na.ma(snr_up, k = 4, weighting = "linear"))

NodeData_n <- NodeData_n %>%
  group_by(node_name) %>%
  mutate(avg_ma_sma = na.ma(snr_up, k = 4, weighting = "simple"))

# Calculate the Root Mean Square error after imputation

rmse_kalman <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_kalman))^2)
rmse_mean <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_mean))^2)
rmse_median <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_median))^2)
rmse_mode <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_mode))^2)
rmse_random <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_random))^2)
rmse_locf <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_locf))^2)
rmse_nocb <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_nocb))^2)
rmse_interp_linear <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_interp_linear))^2)
rmse_interp_spline <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_interp_spline))^2)
rmse_ma_expo <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_ma_expo))^2)
rmse_ma_linear <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_ma_linear))^2)
rmse_ma_sma <- sqrt((mean(NodeData_n$snr_up, na.rm = T) - mean(NodeData_n$avg_ma_sma))^2)



rmse_node <- data.frame(methods=c('Kalman', 'Mean', 'Median', 'Mode',
        'Random', 'LOCF', 'NOCB', 'Linear', 'Spline', 'MA Exponential',
        'MA Linear', 'Simple Moving Avg'),
          RMSE=c(rmse_kalman, rmse_mean, rmse_median, rmse_mode, rmse_random, rmse_locf, rmse_nocb,
          rmse_interp_linear, rmse_interp_spline, rmse_ma_expo, rmse_ma_linear,
          rmse_ma_sma))

rm(rmse_kalman, rmse_mean, rmse_median, rmse_mode, 
   rmse_random, rmse_locf, rmse_nocb, rmse_interp_linear, 
   rmse_interp_spline, rmse_ma_expo, rmse_ma_linear, rmse_ma_sma)


# Imputing the snr dn and pathloss for each mac through different methods
# (Interpolation, Moving Avergages etc...)


# Here we are using only the mac adresses present in the mac_feature_list_updated
# dataset. It contains mac addresses which have atleast 50% of non NA values in
# snr_dn and pathloss variables.

# Creating the Mac series (src_node_id) for snr_dn. 

mac_snrdn_n <-
  CPEData_n[, c("node_name", "src_node_id", "hour_stamp", "snr_dn")]
mac_snrdn <-
  subset(mac_snrdn,
         src_node_id %in% mac_feature_list_updated$src_node_id)

mac_snrdn_n <-
  mac_snrdn[order(mac_snrdn$node_name,
                  mac_snrdn$src_node_id,
                  mac_snrdn$hour_stamp), ]


# Creating the Mac series (src_node_id) for pathloss

mac_pathloss_n <-   CPEData_n[, c("node_name", "src_node_id", "hour_stamp", "pathloss")]

mac_pathloss_n <-  subset(mac_pathloss,
         src_node_id %in% mac_feature_list_updated$src_node_id)

mac_pathloss_n <-   mac_pathloss[order(mac_pathloss$node_name,
                     mac_pathloss$src_node_id,
                     mac_pathloss$hour_stamp), ]

# Imputing the snr dn for each macs from the values through different 
# methods (Interpolation, Moving Avergages etc...)

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_kalman = na.kalman(snr_dn))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_mean = na.mean(snr_dn, option = "mean"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_median = na.mean(snr_dn, option = "median"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_mode = na.mean(snr_dn, option = "mode"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_random = na.random(snr_dn))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_locf = na.locf(snr_dn, option = "locf"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_nocb = na.locf(snr_dn, option = "nocb"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_interp_linear = na.interpolation(snr_dn, option = "linear"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_interp_spline = na.interpolation(snr_dn, option = "spline"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_ma_expo = na.ma(snr_dn, k = 4, weighting = "exponential"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_ma_linear = na.ma(snr_dn, k = 4, weighting = "linear"))

mac_snrdn_n <- mac_snrdn_n %>%
  group_by(src_node_id) %>%
  mutate(snr_dn_ma_sma = na.ma(snr_dn, k = 4, weighting = "simple"))

# Calculate the Root Mean Square error after imputation

rmse_kalman <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_kalman))^2)
rmse_mean <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_mean))^2)
rmse_median <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_median))^2)
rmse_mode <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_mode))^2)
rmse_random <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_random))^2)
rmse_locf <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_locf))^2)
rmse_nocb <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_nocb))^2)
rmse_interp_linear <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_interp_linear))^2)
rmse_interp_spline <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_interp_spline))^2)
rmse_ma_expo <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_ma_expo))^2)
rmse_ma_linear <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_ma_linear))^2)
rmse_ma_sma <- sqrt((mean(mac_snrdn_n$snr_dn, na.rm = T) - mean(mac_snrdn_n$snr_dn_ma_sma))^2)


rmse_snrdn <- data.frame(methods=c('Kalman', 'Mean', 'Median', 'Mode', 'Random',
      'LOCF', 'NOCB', 'Linear', 'Spline', 'MA Exponential', 'MA Linear',
      'Simple Moving Avg'), 
      RMSE=c(rmse_kalman,rmse_mean, rmse_median, rmse_mode, rmse_random,
             rmse_locf, rmse_nocb, rmse_interp_linear, rmse_interp_spline,
             rmse_ma_expo, rmse_ma_linear, rmse_ma_sma
      ))

rm(rmse_kalman, rmse_mean, rmse_median, rmse_mode, rmse_random,
   rmse_locf, rmse_nocb, rmse_interp_linear, rmse_interp_spline, rmse_ma_expo,
   rmse_ma_linear, rmse_ma_sma)


# Imputing the pathloss for each macs from the values through different 
# methods (Interpolation, Moving Avergages etc...)

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_kalman = na.kalman(pathloss))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_mean = na.mean(pathloss, option = "mean"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_median = na.mean(pathloss, option = "median"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_mode = na.mean(pathloss, option = "mode"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_random = na.random(pathloss))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_locf = na.locf(pathloss, option = "locf"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_nocb = na.locf(pathloss, option = "nocb"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_interp_linear = na.interpolation(pathloss, option = "linear"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_interp_spline = na.interpolation(pathloss, option = "spline"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_ma_expo = na.ma(pathloss, k = 4, weighting = "exponential"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_ma_linear = na.ma(pathloss, k = 4, weighting = "linear"))

mac_pathloss_n <- mac_pathloss_n %>%
  group_by(src_node_id) %>%
  mutate(pathloss_ma_sma = na.ma(pathloss, k = 4, weighting = "simple"))


# Calculate the Root Mean Square error after imputation


rmse_kalman <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_kalman))^2)
rmse_mean <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_mean))^2)
rmse_median <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_median))^2)
rmse_mode <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_mode))^2)
rmse_random <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_random))^2)
rmse_locf <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_locf))^2)
rmse_nocb <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_nocb))^2)
rmse_interp_linear <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_interp_linear))^2)
rmse_interp_spline <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_interp_spline))^2)
rmse_ma_expo <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_ma_expo))^2)
rmse_ma_linear <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_ma_linear))^2)
rmse_ma_sma <- sqrt((mean(mac_pathloss_n$pathloss, na.rm = T) - mean(mac_pathloss_n$pathloss_ma_sma))^2)


rmse_pathloss <- data.frame(methods=c('Kalman', 'Mean', 'Median', 'Mode', 'Random',
'LOCF', 'NOCB', 'Linear', 'Spline', 'MA Exponential', 'MA Linear',                                   'Simple Moving Avg'), 
                         RMSE=c(rmse_kalman,rmse_mean, rmse_median, rmse_mode, rmse_random,
                                rmse_locf, rmse_nocb, rmse_interp_linear, rmse_interp_spline,
                                rmse_ma_expo, rmse_ma_linear, rmse_ma_sma
                         ))


rm(rmse_kalman, rmse_mean, rmse_median, rmse_mode, rmse_random,
   rmse_locf, rmse_nocb, rmse_interp_linear, rmse_interp_spline, rmse_ma_expo,
   rmse_ma_linear, rmse_ma_sma)
