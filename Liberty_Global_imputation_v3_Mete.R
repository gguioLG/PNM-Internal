#################################################
## STEPS FOLLOWED
#################################################

# 1. SETTING UP R AND CONNECTION TO THE PostgreSQL DATABASE
# 2. REMOVING DUPLICATE ROWS FROM CPE DATA
# 3. CREATING THE FEATURES INDEPENDENTLY
# 4. MERGING THE FEATURE LIST
# 5. REMOVING MAC ADRESSES WHERE PERCENTAGE NA VALUES FOR SNR_DN OR PATHLOSS 
#    IS GREATER THAN 50%. CREATING AN UPDATED FEATURE LIST
# 6. IMPUTATION ON ALL MACs PRESENT IN THE UPDATED FEATURE LIST
# 7. CALCULATING TIME SERIES SIMILARITY DISTANCE MEASURES ON ALL MACs PRESENT IN THE UPDATED FEATURE LIST
# 8. INCULDING THE TIME SERIES DISTANCE METRICS IN THE UPDATE FEATURE LIST
# 9. FEATURE IMPORTANCE
# 10.CLASSIFICATION ALGORITHM - WORK IN PROGRESS

#################################################
## SETTING UP R AND CONNECTION TO THE DB
#################################################

#Use local library
.libPaths("/app/RPackageLibrary/R-3.3")

#Package Calls
library(RPostgreSQL)
library(imputeTS)
library(dplyr)
library(data.table)
library(plyr)
library(stringr)
library(reshape2)
library(TSDist)
library(caret)

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

#Node Data set - Filter out the time series data from polling dataset for all probelamtic nodes. 
# The snr_up for each node is calculated by averaging snr_up for each CPE under a node.

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

#################################################
## REMOVING DUPLICATE ROWS FROM CPE DATA. 
## WE NEED TO REMOVE THE REFERENCE_DATE COLUMN WHICH IS CAUSING THE DUPLICATES.
#################################################

CPEData_n <- unique(CPEData[,-1])

#################################################
## CREATING THE FEATURES INDEPENDENTLY
#################################################

#Length of Mac series - Total number of rows in the polling data set

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

# Average difference between successive time stamp (hour_stamp) for non na snr dn values -

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

# Average difference between successive time stamp (hour_stamp) for non na pathloss values -

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

#################################################
## MERGING THE FEATURE LIST
#################################################

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


# #################################################
# ## REMOVING MAC ADRESSES WHERE PERCENTAGE NA VALUES FOR SNR_DN OR PATHLOSS 
# ## IS GREATER THAN 50%. CREATING AN UPDATED FEATURE LIST
# #################################################
# 
# #mac_feature_list_updated <-
# #  subset(mac_feature_list , perc_snrdn_na <= 0.5)
# 
# #mac_feature_list_updated <-
# #  subset(mac_feature_list_updated , perc_pathloss_na <= 0.5)
# 
# #################################################
# ## IMPUTATION
# #################################################
# 
# # Imputing the NA values in the node dataset with Simple moving averages(SMA). 
# # The SMA method was chosen since it was performing better than most of the 
# # other methods based on RMSE metric.
# 
NodeData <- NodeData[order(NodeData$node_name, NodeData$hour_stamp), ]
# 
# NodeData <- NodeData %>%
#   group_by(node_name) %>%
#   mutate(snr_up_sma = na.ma(snr_up, k = 4, weighting = "simple"))
# 
# # Creating the Mac series (src_node_id) for snr_dn
# 
mac_snrdn <-  CPEData_n[, c("node_name", "src_node_id", "hour_stamp", "snr_dn")]
mac_snrdn <-  subset(mac_snrdn, src_node_id %in% mac_feature_list_$src_node_id)

# # Creating the Mac series (src_node_id) for pathloss

mac_pathloss <- CPEData_n[, c("node_name", "src_node_id", "hour_stamp", "pathloss")]
mac_pathloss <- subset(mac_pathloss, src_node_id %in% mac_feature_list$src_node_id)

# # Imputing the NA values in the Mac snrdn dataset with Simple moving averages
# 
mac_snrdn <-  mac_snrdn[order(mac_snrdn$node_name,
                              mac_snrdn$src_node_id,
                              mac_snrdn$hour_stamp), ]
# 
# mac_snrdn <- mac_snrdn %>%
#   group_by(src_node_id) %>%
#   mutate(snr_dn_sma = na.ma(snr_dn, k = 4, weighting = "simple"))
# 
# # Imputing the NA values in the Mac pathloss dataset with Simple moving averages
# 
mac_pathloss <-
  mac_pathloss[order(mac_pathloss$node_name,
                     mac_pathloss$src_node_id,
                     mac_pathloss$hour_stamp), ]
# 
# mac_pathloss <- mac_pathloss %>%
#   group_by(src_node_id) %>%
#   mutate(pathloss_sma = na.ma(pathloss, k = 4, weighting = "simple"))
# 
# 
# #################################################
# ## CALCULATING TIME SERIES SIMILARITY DISTANCE MEASURES ON ALL MACs 
# ## PRESENT IN THE UPDATED FEATURE LIST
# #################################################
# 
# # Creating a unique list of nodes and mac addresses
# 
# node_mac <- unique(CPEData_n[, c("node_name", "src_node_id")])
# node_mac <-  subset(node_mac, src_node_id %in% mac_feature_list$src_node_id)
# 
# # Shape-based distances - Dynamic Time Warping (DTW) Distance
# 
# for (i in 1:nrow(node_mac))
# {
#   
#   node_series <- NA
#   mac_snrdn_series  <- NA
#   mac_pathloss_series  <- NA
#   
#   node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
#   mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
#   mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
#   
#   node_mac$snrdn_dtw[i] <- TSDistances(node_series, mac_snrdn_series , distance="dtw", sigma=10)
#   node_mac$pathloss_dtw[i] <- TSDistances(node_series, mac_pathloss_series , distance="dtw",sigma=10)
# }
# 
# 
# # Edit based distances - Edit Distance for Real Sequences (EDR)
# 
# for (i in 1:nrow(node_mac))
# {
#   
#   node_series <- NA
#   mac_snrdn_series  <- NA
#   mac_pathloss_series  <- NA
#   
#   node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
#   mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
#   mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
#   
#   node_mac$snrdn_edr[i] <- TSDistances(node_series, mac_snrdn_series , distance="edr", epsilon=0.1)
#   node_mac$pathloss_edr[i] <- TSDistances(node_series, mac_pathloss_series , distance="edr", epsilon=0.1)
# }
# 
# # Edit based distances - Longest Common Subsequence (LCSS) 
# 
# for (i in 1:nrow(node_mac))
# {
#   
#   node_series <- NA
#   mac_snrdn_series  <- NA
#   mac_pathloss_series  <- NA
#   
#   node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
#   mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
#   mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
#   
#   node_mac$snrdn_lcss[i] <- TSDistances(node_series, mac_snrdn_series , distance="lcss", epsilon=0.1)
#   node_mac$pathloss_lcss[i] <- TSDistances(node_series, mac_pathloss_series , distance="lcss", epsilon=0.1)
# }
# 
# # Edit based distances - Edit Distance with Real Penalty (ERP) 
# 
# for (i in 1:nrow(node_mac))
# {
#   
#   node_series <- NA
#   mac_snrdn_series  <- NA
#   mac_pathloss_series  <- NA
#   
#   node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
#   mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
#   mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
#   
#   node_mac$snrdn_erp[i] <- TSDistances(node_series, mac_snrdn_series , distance="erp", g=0)
#   node_mac$pathloss_erp[i] <- TSDistances(node_series, mac_pathloss_series , distance="erp", g=0)
# }
# 
# 
# # Feaure-based distances - Cross-correlation based 
# 
# for (i in 1:nrow(node_mac))
# {
#   
#   node_series <- NA
#   mac_snrdn_series  <- NA
#   mac_pathloss_series  <- NA
#   
#   node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
#   mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
#   mac_pathloss_series <- mac_pathloss$pathloss_sma[mac_pathloss$src_node_id == node_mac$src_node_id[i]]      
#   
#   node_mac$snrdn_ccor[i] <- TSDistances(node_series, mac_snrdn_series , distance="ccor")
#   node_mac$pathloss_ccor[i] <- TSDistances(node_series, mac_pathloss_series , distance="ccor")
# }
# 
# rm(node_series)
# rm(mac_snrdn_series)
# rm(mac_pathloss_series)
# 
# ########################################################################
# ## INCULDING THE TIME SERIES DISTANCE METRICS IN THE UPDATE FEATURE LIST
# ########################################################################
# 
# # Adding TS similarity columns to feature list
# 
# mac_feature_list$snrdn_dtw <- node_mac$snrdn_dtw[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$pathloss_dtw <- node_mac$pathloss_dtw[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$snrdn_edr <- node_mac$snrdn_edr[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$pathloss_edr <- node_mac$pathloss_edr[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$snrdn_lcss <- node_mac$snrdn_lcss[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$pathloss_lcss <- node_mac$pathloss_lcss[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$snrdn_erp <- node_mac$snrdn_erp[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$pathloss_erp <- node_mac$pathloss_erp[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$snrdn_ccor <- node_mac$snrdn_ccor[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# mac_feature_list$pathloss_ccor <- node_mac$pathloss_ccor[match(mac_feature_list$src_node_id, node_mac$src_node_id)]
# 
# # Adding mac label - 0/1 variable for CPD cases. 
# 
# mac_feature_list$mac_label <- mac_positive$mac_label[match(mac_feature_list$src_node_id, mac_positive$src_node_id)]
# 
# mac_feature_list$mac_label[is.na(mac_feature_list$mac_label)] <- 0
# 
# #################################################
# ## FEATURE IMPORTANCE
# #################################################
# 
# # Making a new data set to preserve the original
# 
# mac_new <- mac_feature_list
# 
# #Replace Inf with the median of the pathloss_rxpwrup_ratio column
# 
# med <- median(mac_new[mac_new$pathloss_rxpwrup_ratio!=Inf,"pathloss_rxpwrup_ratio"])
# mac_new[mac_new$pathloss_rxpwrup_ratio==Inf, "pathloss_rxpwrup_ratio"] <- med
# 
# 
# #Replace NA values of snrdn_ccor with median
# 
# med <- median(mac_new[!is.na(mac_new$snrdn_ccor),"snrdn_ccor"])
# mac_new[is.na(mac_new$snrdn_ccor), "snrdn_ccor"] <- med
# 
# mac_new$mac_label <- as.factor(mac_new$mac_label)
# 
# # Rank Features By Importance using Learning Vector Quantization (LVQ) model 
# # and Random Forest model. # The varImp is then used to estimate the variable
# # importance.
# 
# set.seed(1)
# 
# control <- caret::trainControl(method="repeatedcv", number=10, repeats=3)
# 
# model_lvq <- caret::train(mac_label~., data=mac_new[,-1], method="lvq", preProcess="scale", trControl=control)
# model_rf <- caret::train(mac_label~., data=mac_new[,-1], method="rf", preProcess="scale", trControl=control)
# 
# importance_lvq <- varImp(model_lvq, scale=FALSE)
# importance_rf <- varImp(model_rf, scale=FALSE)
# 
# plot(importance_lvq)
# plot(importance_rf)
# 
# # Feature Selection - Recursive Feature Elimination or RFE. A Random Forest 
# # algorithm is used on each iteration to evaluate the model. The algorithm is
# # configured to explore all possible subsets of the attributes. All 32 
# # attributes are selected, although in the plot showing the accuracy of the
# # different attribute subset sizes, we can see that just 4 attributes gives
# # almost comparable results.
# 
# set.seed(1)
# 
# control <- rfeControl(functions=rfFuncs, method="cv", number=10)
# results <- rfe(mac_new[,2:33], mac_new[,34], sizes=c(1:32), rfeControl=control)
# 
# print(results)
# 
# plot(results, type=c("g", "o"))
# 
# #################################################
# ## PRINCIPAL COMPONENT ANALYSIS
# #################################################
# 
# # Identify highly correlated features
# 
# correlationMatrix <- cor(mac_new[,-c(1,34)])
# 
# corrplot(cor(mac_new[,-c(1,34)]))
# 
# set.seed(1)
# 
# prin_comp <- prcomp(mac_new[, !(colnames(mac_new) %in% c("src_node_id","mac_label"))], scale. = T, center = T )
# 
# summary(prin_comp)
# std_dev <- prin_comp$sdev
# pr_var <- std_dev^2
# prop_varex <- pr_var/sum(pr_var)
# 
# #cumulative scree plot
# 
# plot(cumsum(prop_varex), xlab = "Principal Component",
#      ylab = "Cumulative Proportion of Variance Explained",
#      type = "b")
# 
# # Results show that 14 variables out of 32 explains around 95% of the variance
# 
# #################################################
# ## CLASSIFICATION ALGORITHM - WORK IN PROGRESS
# #################################################
# 
# # Retaining the first 14 principal components
# 
# df_pca <- as.data.frame(prin_comp$x[,c(1:14)])
# df_pca$mac_label <- as.factor(mac_new$mac_label)

# -------- Mete / Bucketization for imputation methods --------

# 16. Impute the snr_dn and pathloss  values according to the missing values in a new column in mac_snrdn and mac_pathloss. 
# Here we have to devise different strategy of imputation depending on the % missing values. So we might need to perform different imputation strategy for macs which has less than 50% of NA values and for macs greater than 50% of NA values. 
# This is just example, we have to bin the macs and then decide how to impute for each bin.

# Operation steps;
# 1. Add new columns
# 2. Bucketize on NA percentages
# 3. Develop an imputation method for each bucket and assign it to the corresponding bucket.

# Package calls
library(imputeTS)

# New (updated) dataframes
mac_pathloss_buckets <- mac_pathloss
mac_snrdn_buckets <- mac_snrdn

# 1. New columns for NA percentage, buckets and methods.

# 1.1 Adding new columns to mac_snrdn and mac_pathloss dataframes;
# Adding new columns for NA percentage.
mac_pathloss_buckets$newcolumn<-NA
mac_snrdn_buckets$newcolumn<-NA
names(mac_pathloss_buckets)[names(mac_pathloss_buckets) == 'newcolumn'] <- 'perc_pathloss_na'
names(mac_snrdn_buckets)[names(mac_snrdn_buckets) == 'newcolumn'] <- 'perc_snrdn_na'

# <Note: unique mac addresses>

# Adding new columns for Buckets.
mac_pathloss_buckets$newcolumn<-NA
mac_snrdn_buckets$newcolumn<-NA
names(mac_pathloss_buckets)[names(mac_pathloss_buckets) == 'newcolumn'] <- 'Bucket'
names(mac_snrdn_buckets)[names(mac_snrdn_buckets) == 'newcolumn'] <- 'Bucket'

# Adding new columns for Methods.
mac_pathloss_buckets$newcolumn<-NA
mac_snrdn_buckets$newcolumn<-NA
names(mac_pathloss_buckets)[names(mac_pathloss_buckets) == 'newcolumn'] <- 'Method'
names(mac_snrdn_buckets)[names(mac_snrdn_buckets) == 'newcolumn'] <- 'Method'

# Filling the values for perc_pathloss_NA and perc_snrdn_NA columns.

mac_pathloss_buckets$perc_pathloss_na <- mac_feature_list$perc_pathloss_na[match(mac_pathloss_buckets$src_node_id, mac_feature_list$src_node_id)]
mac_snrdn_buckets$perc_snrdn_na <- mac_feature_list$perc_snrdn_na[match(mac_snrdn_buckets$src_node_id, mac_feature_list$src_node_id)]

# 1.2 Bucketting into 5 intervals.

# 1.2.1 Bucketing the mac_snr_dn_na values.
mac_snrdn_buckets$perc_snrdn_na <- as.numeric(mac_snrdn_buckets$perc_snrdn_na)
mac_snrdn_buckets$Bucket <- cut(mac_snrdn_buckets$perc_snrdn_na, 5, include.lowest = TRUE, labels = FALSE)
mac_snrdn_buckets <- as.data.frame(mac_snrdn_buckets)

# 1.2.2 Bucketing the mac_pathloss_na values.
mac_pathloss_buckets$perc_pathloss_na <- as.numeric(mac_pathloss_buckets$perc_pathloss_na)
mac_pathloss_buckets$Bucket <- cut(mac_pathloss_buckets$perc_pathloss_na, 5, include.lowest = TRUE, labels = FALSE)
mac_pathloss_buckets <- as.data.frame(mac_pathloss_buckets)

# 2. Developing the NA imputation method strategies depending on the bucket, and assigning & applying the imputation methodologies to the buckets;

# # Subsets pathloss
# Pathloss_Bucket1_df <- subset(mac_pathloss_buckets, Bucket == 1)
# Pathloss_Bucket2_df <- subset(mac_pathloss_buckets, Bucket == 2)
# Pathloss_Bucket3_df <- subset(mac_pathloss_buckets, Bucket == 3)
# Pathloss_Bucket4_df <- subset(mac_pathloss_buckets, Bucket == 4)
# Pathloss_Bucket5_df <- subset(mac_pathloss_buckets, Bucket == 5)
# 
# # Subsets snrdn
# Snrdn_Bucket1_df <- subset(mac_snrdn_buckets, Bucket == 1)
# Snrdn_Bucket2_df <- subset(mac_snrdn_buckets, Bucket == 2)
# Snrdn_Bucket3_df <- subset(mac_snrdn_buckets, Bucket == 3)
# Snrdn_Bucket4_df <- subset(mac_snrdn_buckets, Bucket == 4)
# Snrdn_Bucket5_df <- subset(mac_snrdn_buckets, Bucket == 5)

# 2.1 Distribution and statistics of NAs in mac addresses snr_dn and pathloss features;
# plotNA.distribution(mac_pathloss_buckets$pathloss)
# plotNA.distribution(mac_snrdn_buckets$snr_dn)
# statsNA.distribution(mac_pathloss_buckets$pathloss)
# statsNA.distribution(mac_snrdn_buckets$snr_dn)

# 2.2 Various Imputation methods for each bucket.
# Possible methods and their usage on the two dataframes;

################################### A. INTERPOLATION #############################################
# SNr_DN
# Bucket 1, RMSE: 8.603
Temp <- subset(mac_snrdn_buckets, Bucket == 1)
Snrdn_Bucket1_df_interpolated <- na.interpolation(Temp$snr_dn, option = "linear")
Snrdn_Bucket1_df_interpolated <- as.data.frame(Snrdn_Bucket1_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket1_interpolation <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket1_df_interpolated$Snrdn_Bucket1_df_interpolated)^2))
RMSE_snrdn_bucket1_interpolation <- as.numeric(RMSE_snrdn_bucket1_interpolation)

# Bucket 2, RMSE: 21.315
Temp <- subset(mac_snrdn_buckets, Bucket == 2)
Snrdn_Bucket2_df_interpolated <- na.interpolation(Temp$snr_dn, option = "linear")
Snrdn_Bucket2_df_interpolated <- as.data.frame(Snrdn_Bucket2_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket2_interpolation <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket2_df_interpolated$Snrdn_Bucket2_df_interpolated)^2))
RMSE_snrdn_bucket2_interpolation <- as.numeric(RMSE_snrdn_bucket2_interpolation)

# Bucket 3, RMSE: 26.478
Temp <- subset(mac_snrdn_buckets, Bucket == 3)
Snrdn_Bucket3_df_interpolated <- na.interpolation(Temp$snr_dn, option = "linear")
Snrdn_Bucket3_df_interpolated <- as.data.frame(Snrdn_Bucket3_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket3_interpolation <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket3_df_interpolated$Snrdn_Bucket3_df_interpolated)^2))
RMSE_snrdn_bucket3_interpolation <- as.numeric(RMSE_snrdn_bucket3_interpolation)

# Bucket 4, RMSE: 31.430
Temp <- subset(mac_snrdn_buckets, Bucket == 4)
Snrdn_Bucket4_df_interpolated <- na.interpolation(Temp$snr_dn, option = "linear")
Snrdn_Bucket4_df_interpolated <- as.data.frame(Snrdn_Bucket4_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket4_interpolation <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket4_df_interpolated$Snrdn_Bucket4_df_interpolated)^2))
RMSE_snrdn_bucket4_interpolation <- as.numeric(RMSE_snrdn_bucket4_interpolation)

# Bucket 5, RMSE: 36.820
Temp <- subset(mac_snrdn_buckets, Bucket == 5)
Snrdn_Bucket5_df_interpolated <- na.interpolation(Temp$snr_dn, option = "linear")
Snrdn_Bucket5_df_interpolated <- as.data.frame(Snrdn_Bucket5_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket5_interpolation <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket5_df_interpolated$Snrdn_Bucket5_df_interpolated)^2))
RMSE_snrdn_bucket5_interpolation <- as.numeric(RMSE_snrdn_bucket5_interpolation)

# PATHLOSS
# Bucket 1, RMSE: 9.331
Temp <- subset(mac_pathloss_buckets, Bucket == 1)
Pathloss_Bucket1_df_interpolated <- na.interpolation(Temp$pathloss, option = "linear")
Pathloss_Bucket1_df_interpolated <- as.data.frame(Pathloss_Bucket1_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket1_interpolation <- sqrt(mean((Temp$pathloss - Pathloss_Bucket1_df_interpolated$Pathloss_Bucket1_df_interpolated)^2))
RMSE_pathloss_bucket1_interpolation <- as.numeric(RMSE_pathloss_bucket1_interpolation)

# Bucket 2, RMSE: 22.989
Temp <- subset(mac_pathloss_buckets, Bucket == 2)
Pathloss_Bucket2_df_interpolated <- na.interpolation(Temp$pathloss, option = "linear")
Pathloss_Bucket2_df_interpolated <- as.data.frame(Pathloss_Bucket2_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket2_interpolation<- sqrt(mean((Temp$pathloss - Pathloss_Bucket2_df_interpolated$Pathloss_Bucket2_df_interpolated)^2))
RMSE_pathloss_bucket2_interpolation <- as.numeric(RMSE_pathloss_bucket2_interpolation)

# Bucket 3, RMSE: 29.390
Temp <- subset(mac_pathloss_buckets, Bucket == 3)
Pathloss_Bucket3_df_interpolated <- na.interpolation(Temp$pathloss, option = "linear")
Pathloss_Bucket3_df_interpolated <- as.data.frame(Pathloss_Bucket3_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket3_interpolation <- sqrt(mean((Temp$pathloss - Pathloss_Bucket3_df_interpolated$Pathloss_Bucket3_df_interpolated)^2))
RMSE_pathloss_bucket3_interpolation <- as.numeric(RMSE_pathloss_bucket3_interpolation)

# Bucket 4, RMSE: 35.473
Temp <- subset(mac_pathloss_buckets, Bucket == 4)
Pathloss_Bucket4_df_interpolated <- na.interpolation(Temp$pathloss, option = "linear")
Pathloss_Bucket4_df_interpolated <- as.data.frame(Pathloss_Bucket4_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket4_interpolation <- sqrt(mean((Temp$pathloss - Pathloss_Bucket4_df_interpolated$Pathloss_Bucket4_df_interpolated)^2))
RMSE_pathloss_bucket4_interpolation <- as.numeric(RMSE_pathloss_bucket4_interpolation)

# Bucket 5, RMSE: 41.438
Temp <- subset(mac_pathloss_buckets, Bucket == 5)
Pathloss_Bucket5_df_interpolated <- na.interpolation(Temp$pathloss, option = "linear")
Pathloss_Bucket5_df_interpolated <- as.data.frame(Pathloss_Bucket5_df_interpolated)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket5_interpolation <- sqrt(mean((Temp$pathloss - Pathloss_Bucket5_df_interpolated$Pathloss_Bucket5_df_interpolated)^2))
RMSE_pathloss_bucket5_interpolation <- as.numeric(RMSE_pathloss_bucket5_interpolation)

############################################# B. MEAN ##################################################
# SNr_DN

# Bucket 1, RMSE: 8.603
Temp <- subset(mac_snrdn_buckets, Bucket == 1)
Snrdn_Bucket1_df_mean <- na.mean(Temp$snr_dn, option = "mean")
Snrdn_Bucket1_df_mean <- as.data.frame(Snrdn_Bucket1_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket1_mean <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket1_df_mean$Snrdn_Bucket1_df_mean)^2))
RMSE_snrdn_bucket1_mean <- as.numeric(RMSE_snrdn_bucket1_mean)

# Bucket 2, RMSE: 21.364
Temp <- subset(mac_snrdn_buckets, Bucket == 2)
Snrdn_Bucket2_df_mean <- na.mean(Temp$snr_dn, option = "mean")
Snrdn_Bucket2_df_mean <- as.data.frame(Snrdn_Bucket2_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket2_mean <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket2_df_mean$Snrdn_Bucket2_df_mean)^2))
RMSE_snrdn_bucket2_mean <- as.numeric(RMSE_snrdn_bucket2_mean)

# Bucket 3, RMSE: 26.478
Temp <- subset(mac_snrdn_buckets, Bucket == 3)
Snrdn_Bucket3_df_mean <- na.mean(Temp$snr_dn, option = "mean")
Snrdn_Bucket3_df_mean <- as.data.frame(Snrdn_Bucket3_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket3_mean <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket3_df_mean$Snrdn_Bucket3_df_mean)^2))
RMSE_snrdn_bucket3_mean <- as.numeric(RMSE_snrdn_bucket3_mean)

# Bucket 4, RMSE: 31.423
Temp <- subset(mac_snrdn_buckets, Bucket == 4)
Snrdn_Bucket4_df_mean <- na.mean(Temp$snr_dn, option = "mean")
Snrdn_Bucket4_df_mean <- as.data.frame(Snrdn_Bucket4_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket4_mean <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket4_df_mean$Snrdn_Bucket4_df_mean)^2))
RMSE_snrdn_bucket4_mean <- as.numeric(RMSE_snrdn_bucket4_mean)

# Bucket 5, RMSE: 36.837
Temp <- subset(mac_snrdn_buckets, Bucket == 5)
Snrdn_Bucket5_df_mean <- na.mean(Temp$snr_dn, option = "mean")
Snrdn_Bucket5_df_mean <- as.data.frame(Snrdn_Bucket5_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket5_mean <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket5_df_mean$Snrdn_Bucket5_df_mean)^2))
RMSE_snrdn_bucket5_mean <- as.numeric(RMSE_snrdn_bucket5_mean)

# PATHLOSS

# Bucket 1, RMSE: 9.182
Temp <- subset(mac_pathloss_buckets, Bucket == 1)
Pathloss_Bucket1_df_mean <- na.mean(Temp$pathloss, option = "mean")
Pathloss_Bucket1_df_mean <- as.data.frame(Pathloss_Bucket1_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket1_mean <- sqrt(mean((Temp$pathloss - Pathloss_Bucket1_df_mean$Pathloss_Bucket1_df_mean)^2))
RMSE_pathloss_bucket1_mean <- as.numeric(RMSE_pathloss_bucket1_mean)

# Bucket 2, RMSE: 22.768
Temp <- subset(mac_pathloss_buckets, Bucket == 2)
Pathloss_Bucket2_df_mean <- na.mean(Temp$pathloss, option = "mean")
Pathloss_Bucket2_df_mean <- as.data.frame(Pathloss_Bucket2_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket2_mean <- sqrt(mean((Temp$pathloss - Pathloss_Bucket2_df_mean$Pathloss_Bucket2_df_mean)^2))
RMSE_pathloss_bucket2_mean <- as.numeric(RMSE_pathloss_bucket2_mean)

# Bucket 3, RMSE: 29.055
Temp <- subset(mac_pathloss_buckets, Bucket == 3)
Pathloss_Bucket3_df_mean <- na.mean(Temp$pathloss, option = "mean")
Pathloss_Bucket3_df_mean <- as.data.frame(Pathloss_Bucket3_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket3_mean <- sqrt(mean((Temp$pathloss - Pathloss_Bucket3_df_mean$Pathloss_Bucket3_df_mean)^2))
RMSE_pathloss_bucket3_mean <- as.numeric(RMSE_pathloss_bucket3_mean)

# Bucket 4, RMSE: 35.133
Temp <- subset(mac_pathloss_buckets, Bucket == 4)
Pathloss_Bucket4_df_mean <- na.mean(Temp$pathloss, option = "mean")
Pathloss_Bucket4_df_mean <- as.data.frame(Pathloss_Bucket4_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket4_mean <- sqrt(mean((Temp$pathloss - Pathloss_Bucket4_df_mean$Pathloss_Bucket4_df_mean)^2))
RMSE_pathloss_bucket4_mean <- as.numeric(RMSE_pathloss_bucket4_mean)

# Bucket 5, RMSE: 41.300
Temp <- subset(mac_pathloss_buckets, Bucket == 5)
Pathloss_Bucket5_df_mean <- na.mean(Temp$pathloss, option = "mean")
Pathloss_Bucket5_df_mean <- as.data.frame(Pathloss_Bucket5_df_mean)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket5_mean <- sqrt(mean((Temp$pathloss - Pathloss_Bucket5_df_mean$Pathloss_Bucket5_df_mean)^2))
RMSE_pathloss_bucket5_mean <- as.numeric(RMSE_pathloss_bucket5_mean)

################################################## C. KALMAN #############################################
# SNr_DN
# Bucket 1, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 1)
Snrdn_Bucket1_df_kalman <- na.kalman(Temp$snr_dn, model = "StructTS")
Snrdn_Bucket1_df_kalman <- as.data.frame(Snrdn_Bucket1_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket1_kalman <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket1_df_kalman$Snrdn_Bucket1_df_kalman)^2))
RMSE_snrdn_bucket1_kalman <- as.numeric(RMSE_snrdn_bucket1_kalman)

# Bucket 2, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 2)
Snrdn_Bucket2_df_kalman <- na.kalman(Temp$snr_dn, model = "StructTS")
Snrdn_Bucket2_df_kalman <- as.data.frame(Snrdn_Bucket2_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket2_kalman <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket2_df_kalman$Snrdn_Bucket2_df_kalman)^2))
RMSE_snrdn_bucket2_kalman <- as.numeric(RMSE_snrdn_bucket2_kalman)

# Bucket 3, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 3)
Snrdn_Bucket3_df_kalman <- na.kalman(Temp$snr_dn, model = "StructTS")
Snrdn_Bucket3_df_kalman <- as.data.frame(Snrdn_Bucket3_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket3_kalman <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket3_df_kalman$Snrdn_Bucket3_df_kalman)^2))
RMSE_snrdn_bucket3_kalman <- as.numeric(RMSE_snrdn_bucket3_kalman)

# Bucket 4, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 4)
Snrdn_Bucket4_df_kalman <- na.kalman(Temp$snr_dn, model = "StructTS")
Snrdn_Bucket4_df_kalman <- as.data.frame(Snrdn_Bucket4_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket4_kalman <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket4_df_kalman$Snrdn_Bucket4_df_kalman)^2))
RMSE_snrdn_bucket4_kalman <- as.numeric(RMSE_snrdn_bucket4_kalman)

# Bucket 5, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 5)
Snrdn_Bucket5_df_kalman <- na.kalman(Temp$snr_dn, model = "StructTS")
Snrdn_Bucket5_df_kalman <- as.data.frame(Snrdn_Bucket5_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket5_kalman <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket5_df_kalman$Snrdn_Bucket5_df_kalman)^2))
RMSE_snrdn_bucket5_kalman <- as.numeric(RMSE_snrdn_bucket5_kalman)

# PATHLOSS

# Bucket 1, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 1)
Pathloss_Bucket1_df_kalman <- na.kalman(Temp$pathloss, model = "StructTS")
Pathloss_Bucket1_df_kalman <- as.data.frame(Pathloss_Bucket1_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket1_kalman <- sqrt(mean((Temp$pathloss - Pathloss_Bucket1_df_kalman$Pathloss_Bucket1_df_kalman)^2))
RMSE_pathloss_bucket1_kalman <- as.numeric(RMSE_pathloss_bucket1_kalman)

# Bucket 2, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 2)
Pathloss_Bucket2_df_kalman <- na.kalman(Temp$pathloss, model = "StructTS")
Pathloss_Bucket2_df_kalman <- as.data.frame(Pathloss_Bucket2_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket2_kalman <- sqrt(mean((Temp$pathloss - Pathloss_Bucket2_df_kalman$Pathloss_Bucket2_df_kalman)^2))
RMSE_pathloss_bucket2_kalman <- as.numeric(RMSE_pathloss_bucket2_kalman)

# Bucket 3, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 3)
Pathloss_Bucket3_df_kalman <- na.kalman(Temp$pathloss, model = "StructTS")
Pathloss_Bucket3_df_kalman <- as.data.frame(Pathloss_Bucket3_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket3_kalman <- sqrt(mean((Temp$pathloss - Pathloss_Bucket3_df_kalman$Pathloss_Bucket3_df_kalman)^2))
RMSE_pathloss_bucket3_kalman <- as.numeric(RMSE_pathloss_bucket3_kalman)

# Bucket 4, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 4)
Pathloss_Bucket4_df_kalman <- na.kalman(Temp$pathloss, model = "StructTS")
Pathloss_Bucket4_df_kalman <- as.data.frame(Pathloss_Bucket4_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket4_kalman <- sqrt(mean((Temp$pathloss - Pathloss_Bucket4_df_kalman$Pathloss_Bucket4_df_kalman)^2))
RMSE_pathloss_bucket4_kalman <- as.numeric(RMSE_pathloss_bucket4_kalman)

# Bucket 5, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 5)
Pathloss_Bucket5_df_kalman <- na.kalman(Temp$pathloss, model = "StructTS")
Pathloss_Bucket5_df_kalman <- as.data.frame(Pathloss_Bucket5_df_kalman)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket5_kalman <- sqrt(mean((Temp$pathloss - Pathloss_Bucket5_df_kalman$Pathloss_Bucket5_df_kalman)^2))
RMSE_pathloss_bucket5_kalman <- as.numeric(RMSE_pathloss_bucket5_kalman)

######################################## D. MOVING AVERAGE ##############################
# SNr_DN

# Bucket 1, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 1)
Snrdn_Bucket1_df_ma <- na.ma(Temp$snr_dn, k=4, weighting = "simple")
Snrdn_Bucket1_df_ma <- as.data.frame(Snrdn_Bucket1_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket1_ma <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket1_df_ma$Snrdn_Bucket1_df_ma)^2))
RMSE_snrdn_bucket1_ma <- as.numeric(RMSE_snrdn_bucket1_ma)

# Bucket 2, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 2)
Snrdn_Bucket2_df_ma <- na.ma(Temp$snr_dn, k=4, weighting = "simple")
Snrdn_Bucket2_df_ma <- as.data.frame(Snrdn_Bucket2_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket2_ma <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket2_df_ma$Snrdn_Bucket2_df_ma)^2))
RMSE_snrdn_bucket2_ma <- as.numeric(RMSE_snrdn_bucket2_ma)

# Bucket 3, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 3)
Snrdn_Bucket3_df_ma <- na.ma(Temp$snr_dn, k=4, weighting = "simple")
Snrdn_Bucket3_df_ma <- as.data.frame(Snrdn_Bucket3_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket3_ma <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket3_df_ma$Snrdn_Bucket3_df_ma)^2))
RMSE_snrdn_bucket3_ma <- as.numeric(RMSE_snrdn_bucket3_ma)

# Bucket 4, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 4)
Snrdn_Bucket4_df_ma <- na.ma(Temp$snr_dn, k=4, weighting = "simple")
Snrdn_Bucket4_df_ma <- as.data.frame(Snrdn_Bucket4_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket4_ma <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket4_df_ma$Snrdn_Bucket4_df_ma)^2))
RMSE_snrdn_bucket4_ma <- as.numeric(RMSE_snrdn_bucket4_ma)

# Bucket 5, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 5)
Snrdn_Bucket5_df_ma <- na.ma(Temp$snr_dn, k=4, weighting = "simple")
Snrdn_Bucket5_df_ma <- as.data.frame(Snrdn_Bucket5_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket5_ma <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket5_df_ma$Snrdn_Bucket5_df_ma)^2))
RMSE_snrdn_bucket5_ma <- as.numeric(RMSE_snrdn_bucket5_ma)

# PATHLOSS

# Bucket 1, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 1)
Pathloss_Bucket1_df_ma <- na.ma(Temp$pathloss, k=4, weighting = "simple")
Pathloss_Bucket1_df_ma <- as.data.frame(Pathloss_Bucket1_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket1_ma <- sqrt(mean((Temp$pathloss - Pathloss_Bucket1_df_ma$Pathloss_Bucket1_df_ma)^2))
RMSE_pathloss_bucket1_ma <- as.numeric(RMSE_pathloss_bucket1_ma)

# Bucket 2, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 2)
Pathloss_Bucket2_df_ma <- na.ma(Temp$pathloss, k=4, weighting = "simple")
Pathloss_Bucket2_df_ma <- as.data.frame(Pathloss_Bucket2_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket2_ma <- sqrt(mean((Temp$pathloss - Pathloss_Bucket2_df_ma$Pathloss_Bucket2_df_ma)^2))
RMSE_pathloss_bucket2_ma <- as.numeric(RMSE_pathloss_bucket2_ma)

# Bucket 3, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 3)
Pathloss_Bucket3_df_ma <- na.ma(Temp$pathloss, k=4, weighting = "simple")
Pathloss_Bucket3_df_ma <- as.data.frame(Pathloss_Bucket3_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket3_ma <- sqrt(mean((Temp$pathloss - Pathloss_Bucket3_df_ma$Pathloss_Bucket3_df_ma)^2))
RMSE_pathloss_bucket3_ma <- as.numeric(RMSE_pathloss_bucket3_ma)

# Bucket 4, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 4)
Pathloss_Bucket4_df_ma <- na.ma(Temp$pathloss, k=4, weighting = "simple")
Pathloss_Bucket4_df_ma <- as.data.frame(Pathloss_Bucket4_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket4_ma <- sqrt(mean((Temp$pathloss - Pathloss_Bucket4_df_ma$Pathloss_Bucket4_df_ma)^2))
RMSE_pathloss_bucket4_ma <- as.numeric(RMSE_pathloss_bucket4_ma)

# Bucket 5, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 5)
Pathloss_Bucket5_df_ma <- na.ma(Temp$pathloss, k=4, weighting = "simple")
Pathloss_Bucket5_df_ma <- as.data.frame(Pathloss_Bucket5_df_ma)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket5_ma <- sqrt(mean((Temp$pathloss - Pathloss_Bucket5_df_ma$Pathloss_Bucket5_df_ma)^2))
RMSE_pathloss_bucket5_ma <- as.numeric(RMSE_pathloss_bucket5_ma)

######################################## E. LAST OBSERVATION CARRIED FORWARD ##############################
# SNr_DN

# Bucket 1, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 1)
Snrdn_Bucket1_df_locf <- na.locf(Temp$snr_dn, option = "locf", na.remaining = "rev")
Snrdn_Bucket1_df_locf <- as.data.frame(Snrdn_Bucket1_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket1_locf <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket1_df_locf$Snrdn_Bucket1_df_locf)^2))
RMSE_snrdn_bucket1_locf <- as.numeric(RMSE_snrdn_bucket1_locf)

# Bucket 2, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 2)
Snrdn_Bucket2_df_locf <- na.locf(Temp$snr_dn, option = "locf", na.remaining = "rev")
Snrdn_Bucket2_df_locf <- as.data.frame(Snrdn_Bucket2_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket2_locf <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket2_df_locf$Snrdn_Bucket2_df_locf)^2))
RMSE_snrdn_bucket2_locf <- as.numeric(RMSE_snrdn_bucket2_locf)

# Bucket 3, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 3)
Snrdn_Bucket3_df_locf <- na.locf(Temp$snr_dn, option = "locf", na.remaining = "rev")
Snrdn_Bucket3_df_locf <- as.data.frame(Snrdn_Bucket3_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket3_locf <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket3_df_locf$Snrdn_Bucket3_df_locf)^2))
RMSE_snrdn_bucket3_locf <- as.numeric(RMSE_snrdn_bucket3_locf)

# Bucket 4, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 4)
Snrdn_Bucket4_df_locf <- na.locf(Temp$snr_dn, option = "locf", na.remaining = "rev")
Snrdn_Bucket4_df_locf <- as.data.frame(Snrdn_Bucket4_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket4_locf <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket4_df_locf$Snrdn_Bucket4_df_locf)^2))
RMSE_snrdn_bucket4_locf <- as.numeric(RMSE_snrdn_bucket4_locf)

# Bucket 5, RMSE: 
Temp <- subset(mac_snrdn_buckets, Bucket == 5)
Snrdn_Bucket5_df_locf <- na.locf(Temp$snr_dn, option = "locf", na.remaining = "rev")
Snrdn_Bucket5_df_locf <- as.data.frame(Snrdn_Bucket5_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_snrdn_bucket5_locf <- sqrt(mean((Temp$snr_dn - Snrdn_Bucket5_df_locf$Snrdn_Bucket5_df_locf)^2))
RMSE_snrdn_bucket5_locf <- as.numeric(RMSE_snrdn_bucket5_locf)

# PATHLOSS
Pathloss_Bucket1_df_locf <- na.locf(Pathloss_Bucket1_df, option = "locf", na.remaining = "rev")

# Bucket 1, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 1)
Pathloss_Bucket1_df_locf <- na.locf(Temp$pathloss, option = "locf", na.remaining = "rev")
Pathloss_Bucket1_df_locf <- as.data.frame(Pathloss_Bucket1_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket1_locf <- sqrt(mean((Temp$pathloss - Pathloss_Bucket1_df_locf$Pathloss_Bucket1_df_locf)^2))
RMSE_pathloss_bucket1_locf <- as.numeric(RMSE_pathloss_bucket1_locf)

# Bucket 2, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 2)
Pathloss_Bucket2_df_locf <- na.locf(Temp$pathloss, option = "locf", na.remaining = "rev")
Pathloss_Bucket2_df_locf <- as.data.frame(Pathloss_Bucket2_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket2_locf <- sqrt(mean((Temp$pathloss - Pathloss_Bucket2_df_locf$Pathloss_Bucket2_df_locf)^2))
RMSE_pathloss_bucket2_locf <- as.numeric(RMSE_pathloss_bucket2_locf)

# Bucket 3, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 3)
Pathloss_Bucket3_df_locf <- na.locf(Temp$pathloss, option = "locf", na.remaining = "rev")
Pathloss_Bucket3_df_locf <- as.data.frame(Pathloss_Bucket3_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket3_locf <- sqrt(mean((Temp$pathloss - Pathloss_Bucket3_df_locf$Pathloss_Bucket3_df_locf)^2))
RMSE_pathloss_bucket3_locf <- as.numeric(RMSE_pathloss_bucket3_locf)

# Bucket 4, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 4)
Pathloss_Bucket4_df_locf <- na.locf(Temp$pathloss, option = "locf", na.remaining = "rev")
Pathloss_Bucket4_df_locf <- as.data.frame(Pathloss_Bucket4_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket4_locf <- sqrt(mean((Temp$pathloss - Pathloss_Bucket4_df_locf$Pathloss_Bucket1_df_locf)^2))
RMSE_pathloss_bucket4_locf <- as.numeric(RMSE_pathloss_bucket4_locf)

# Bucket 5, RMSE: 
Temp <- subset(mac_pathloss_buckets, Bucket == 1)
Pathloss_Bucket5_df_locf <- na.locf(Temp$pathloss, option = "locf", na.remaining = "rev")
Pathloss_Bucket5_df_locf <- as.data.frame(Pathloss_Bucket5_df_locf)
Temp[is.na(Temp)] <- 0
RMSE_pathloss_bucket5_locf <- sqrt(mean((Temp$pathloss - Pathloss_Bucket1_df_locf$Pathloss_Bucket5_df_locf)^2))
RMSE_pathloss_bucket5_locf <- as.numeric(RMSE_pathloss_bucket5_locf)

# Which one performs the best for each bucket combination?
# Test with RMSE

# rmse(actual, predicted)

# 2.3 Final Imputation

# RESULTING CATEGORIES;

# snr_dn df;

# Bucket 1: 0-20%, Best Method: M1

# Bucket 2: 20-40%, Best Method: M2

# Bucket 3: 40-60%, Best Method: M3

# Bucket 4: 60-80%, Best Method: M4

# Bucket 5: 80-100%, Best Method: M5

# pathloss df;

# Bucket 1: 0-20%, Best Method: 

# Bucket 2: 20-40%, Best Method: M2 

# Bucket 3: 40-60%, Best Method: M3

# Bucket 4: 60-80%, Best Method: M4

# Bucket 5: 80-100%, Best Method: M5