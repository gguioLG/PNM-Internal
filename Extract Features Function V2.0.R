
#Package Calls
library(imputeTS)
library(plyr)
library(data.table)
library(dplyr)
library(stringr)
library(TSdist)

 # #################################################
 # ## FEATURE EXTRACTION FUNCTION
 # #################################################

  ExtractFeaturesFunction <- function(referenceTest, CPEData) 
    
    {
  

  #############################################################
  ## CREATING NODE, SNR_DN AND PATHLOSS TIME SERIES.
  ## SELECTED ONLY 10 DAYS DATA PRIOR TO POLLING REFERENCE DATE.
  #############################################################
  
  # Introducing new fields for date range (10 days)
  
  referenceTest$max_date <- as.Date(referenceTest$polling_reference_date) - 1
  referenceTest$min_date <- as.Date(referenceTest$max_date) - 10
  
  # Filtering CPE Data to have only 10 days of polling data
  
  CPEData$min_date <-  referenceTest$min_date[match(CPEData$node_name,referenceTest$node_name)]
  CPEData$max_date <-  referenceTest$max_date[match(CPEData$node_name,referenceTest$node_name)]
  CPEData$InRange <- as.Date(CPEData$hour_stamp)  >= as.Date(CPEData$min_date, '%m/%d/%Y') & as.Date(CPEData$hour_stamp) <= as.Date(CPEData$max_date, '%m/%d/%Y')
  CPEData <- CPEData[CPEData$InRange == 'TRUE',]
  CPEData <- dplyr::select(CPEData, -c(min_date, max_date, InRange))
  
  # Creating Node time series with 10 days prior data
  
  NodeData <-  setDT(CPEData)[, list(snr_up = mean(snr_up, na.rm = T)),
                              by = .(node_name,hour_stamp)]
  
  NodeData$min_date <-  referenceTest$min_date[match(NodeData$node_name,referenceTest$node_name)]
  NodeData$max_date <-  referenceTest$max_date[match(NodeData$node_name,referenceTest$node_name)]
  NodeData$InRange <- as.Date(NodeData$hour_stamp)  >= as.Date(NodeData$min_date, '%m/%d/%Y') & as.Date(NodeData$hour_stamp) <= as.Date(NodeData$max_date, '%m/%d/%Y')
  NodeData <- NodeData[NodeData$InRange == 'TRUE',]
  NodeData <- NodeData[, c("node_name", "hour_stamp", "snr_up")]
  
  # Creating snrdn time series with 10 days prior data
  
  mac_snrdn <-  CPEData[, c("node_name", "src_node_id", "hour_stamp", "snr_dn")]
  mac_snrdn <- unique(mac_snrdn) 
  mac_snrdn$min_date <-  referenceTest$min_date[match(mac_snrdn$node_name,referenceTest$node_name)]
  mac_snrdn$max_date <-  referenceTest$max_date[match(mac_snrdn$node_name,referenceTest$node_name)]
  mac_snrdn$InRange <- as.Date(mac_snrdn$hour_stamp)  >= as.Date(mac_snrdn$min_date, '%m/%d/%Y') & as.Date(mac_snrdn$hour_stamp) <= as.Date(mac_snrdn$max_date, '%m/%d/%Y')
  mac_snrdn <- mac_snrdn[mac_snrdn$InRange == 'TRUE',]
  mac_snrdn <-  mac_snrdn[, c("node_name", "src_node_id", "hour_stamp", "snr_dn")]
  
  # Creating pathloss time series with 10 days prior data
  
  mac_pathloss <- CPEData[, c("node_name", "src_node_id", "hour_stamp", "pathloss")]
  mac_pathloss <- unique(mac_pathloss) 
  mac_pathloss$min_date <-  referenceTest$min_date[match(mac_pathloss$node_name,referenceTest$node_name)]
  mac_pathloss$max_date <-  referenceTest$max_date[match(mac_pathloss$node_name,referenceTest$node_name)]
  mac_pathloss$InRange <- as.Date(mac_pathloss$hour_stamp)  >= as.Date(mac_pathloss$min_date, '%m/%d/%Y') & as.Date(mac_pathloss$hour_stamp) <= as.Date(mac_pathloss$max_date, '%m/%d/%Y')
  mac_pathloss <- mac_pathloss[mac_pathloss$InRange == 'TRUE',]
  mac_pathloss <-  mac_pathloss[, c("node_name", "src_node_id", "hour_stamp", "pathloss")]
  
  #################################################
  ## CREATING THE STANDARD FEATURES
  #################################################
  
  #Length of Mac series - Total number of rows in the polling data set
  
  mac_len <- ddply(mac_snrdn, .(src_node_id), nrow)
  setnames(mac_len, "V1", "mac_len")
  
  # Missing Values for snr_dn and pathloss
  
  mac_snrdn_na <-
    aggregate(snr_dn ~ src_node_id, data = mac_snrdn, function(x) {
      sum(is.na(x))
    }, na.action = NULL)
  
  setnames(mac_snrdn_na, "snr_dn", "snr_dn_na")
  
  mac_pathloss_na <-
    aggregate(pathloss ~ src_node_id, data = mac_pathloss, function(x) {
      sum(is.na(x))
    }, na.action = NULL)
  
  setnames(mac_pathloss_na, "pathloss", "pathloss_na")
  
  # Descriptive Stats for snr_dn and pathloss, which inlcudes - 
  
  # 1. SNR_DN STANDARD DEVIATION
  # 2. PATHLOSS STANDARD DEVIATION
  # 3. PATHLOSS DELTA - Average difference between successive non na pathloss values
  
  snrdn_stats <-
    setDT(mac_snrdn)[, list(snrdn_sd = sd(snr_dn, na.rm = T)),
                     by = .(src_node_id)]
  
  
  pathloss_stats <-
    setDT(mac_pathloss)[, list(
      pathloss_sd = sd(pathloss, na.rm = T)),
      by = .(src_node_id)]
  
  # Average difference between successive non na pathloss values -
  
  mac_pathloss_delta <-  mac_pathloss %>%
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
  
  
  #################################################
  ## MERGING THE STANDARD FEATURE LIST
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
          y = snrdn_stats,
          by = "src_node_id",
          all.x = TRUE)
  
  mac_feature_list <-
    merge(x = mac_feature_list,
          y = pathloss_stats,
          by = "src_node_id",
          all.x = TRUE)
  
  mac_feature_list <-
    merge(x = mac_feature_list,
          y = mac_pathloss_delta,
          by = "src_node_id",
          all.x = TRUE)
  
  # Removing some of the columns from mac_feature_list
  
  mac_feature_list <- dplyr::select(mac_feature_list,
                                    -c(mac_len, snr_dn_na, pathloss_na))
  
  # Adding node_name to mac_feature_list
  
  node_mac <- unique(CPEData[, c("node_name", "src_node_id")])
  
  mac_feature_list <-
    merge(x = mac_feature_list,
          y = node_mac,
          by = "src_node_id",
          all.x = TRUE)
  
  mac_feature_list <- mac_feature_list %>% select(node_name, everything())
  
  # #################################################
  # ## IMPUTATION OF MISSING VALUES
  # #################################################
  
  # Making a new data set to preserve the original
  
  mac_new <- mac_feature_list
  
  # Removing mac addresses where snr_dn and pathloss NA values are more than 95% 
  
  mac_new <- mac_new[mac_new$perc_snrdn_na < '0.95',]
  mac_new <- mac_new[mac_new$perc_pathloss_na < '0.95',]
  
  # Imputing the NA values in the node dataset with Simple moving averages(SMA).
  # The SMA method was chosen since it was performing better than most of the
  # other methods based on RMSE metric.
  
  setDT(NodeData)
  
  NodeData <- NodeData[order(NodeData$node_name, NodeData$hour_stamp), ]
  
  NodeData[, snr_up_sma := na.ma(snr_up, k = 4, weighting = "simple"),
           by = node_name]
  
  NodeData <- as.data.frame(NodeData)
  
  #### Imputing the NA values in the Mac snrdn dataset
  
  # for less than or equal to 50% NAs, impute by Simple Moving averages
  
  mac_snrdn_1 <- mac_new[mac_new$perc_snrdn_na <= 0.5, ]
  
  mac_snrdn_1 <- mac_snrdn[mac_snrdn$src_node_id %in% mac_snrdn_1$src_node_id,]
  
  mac_snrdn_1 <-  mac_snrdn_1[order(mac_snrdn_1$node_name,
                                    mac_snrdn_1$src_node_id,
                                    mac_snrdn_1$hour_stamp), ]
  
  setDT(mac_snrdn_1)
  
  mac_snrdn_1[, snr_dn_sma :=  na.ma(snr_dn, k = 4, weighting = "simple"),
              by = src_node_id]
  
  mac_snrdn_1 <- as.data.frame(mac_snrdn_1)
  
  # for greater than 50% NAs, impute by median
  
  mac_snrdn_2 <- mac_new[mac_new$perc_snrdn_na > 0.5, ]
  
  mac_snrdn_2 <- mac_snrdn[mac_snrdn$src_node_id %in% mac_snrdn_2$src_node_id,]
  
  mac_snrdn_2 <-  mac_snrdn_2[order(mac_snrdn_2$node_name,
                                    mac_snrdn_2$src_node_id,
                                    mac_snrdn_2$hour_stamp), ]
  
  setDT(mac_snrdn_2)
  
  mac_snrdn_2[, snr_dn_sma :=  replace(snr_dn, is.na(snr_dn),
                                       median(snr_dn, na.rm=TRUE)),
              by = src_node_id]
  
  mac_snrdn_2 <- as.data.frame(mac_snrdn_2)
  
  # Combining the rows
  
  mac_snrdn <- rbind(mac_snrdn_1,mac_snrdn_2)
  
  mac_snrdn <-  mac_snrdn[order(mac_snrdn$node_name,
                                mac_snrdn$src_node_id,
                                mac_snrdn$hour_stamp), ]
  
  mac_snrdn <- as.data.frame(mac_snrdn)
  
  rm(mac_snrdn_1)
  rm(mac_snrdn_2)
  
  
  #### Imputing the NA values in the Mac pathloss dataset
  
  # for less than 50%, impute by SMA
  
  mac_pathloss_1 <- mac_new[mac_new$perc_pathloss_na <= 0.5, ]
  
  mac_pathloss_1 <- mac_pathloss[mac_pathloss$src_node_id %in% mac_pathloss_1$src_node_id,]
  
  mac_pathloss_1 <-  mac_pathloss_1[order(mac_pathloss_1$node_name,
                                          mac_pathloss_1$src_node_id,
                                          mac_pathloss_1$hour_stamp), ]
  
  setDT(mac_pathloss_1)
  
  mac_pathloss_1[, pathloss_sma :=  na.ma(pathloss, k = 4, weighting = "simple"),
                 by = src_node_id]
  
  mac_pathloss_1 <- as.data.frame(mac_pathloss_1)
  
  # for greater than 50%, impute by median
  
  mac_pathloss_2 <- mac_new[mac_new$perc_pathloss_na > 0.5, ]
  
  mac_pathloss_2 <- mac_pathloss[mac_pathloss$src_node_id %in% mac_pathloss_2$src_node_id,]
  
  mac_pathloss_2 <-  mac_pathloss_2[order(mac_pathloss_2$node_name,
                                          mac_pathloss_2$src_node_id,
                                          mac_pathloss_2$hour_stamp), ]
  
  setDT(mac_pathloss_2)
  
  mac_pathloss_2[, pathloss_sma :=  replace(pathloss, is.na(pathloss),
                                            median(pathloss, na.rm=TRUE)),
                 by = src_node_id]
  
  mac_pathloss_2 <- as.data.frame(mac_pathloss_2)
  
  # Combining the rows
  
  mac_pathloss <- rbind(mac_pathloss_1,mac_pathloss_2)
  
  mac_pathloss <-  mac_pathloss[order(mac_pathloss$node_name,
                                      mac_pathloss$src_node_id,
                                      mac_pathloss$hour_stamp), ]
  
  mac_pathloss <- as.data.frame(mac_pathloss)
  
  rm(mac_pathloss_1)
  rm(mac_pathloss_2)
  
  mac_new <- dplyr::select(mac_new,-c(perc_snrdn_na, perc_pathloss_na))
  
  # #################################################
  # ## CALCULATING TIME SERIES SIMILARITY DISTANCE MEASURES ON ALL MACs
  # ## PRESENT IN THE UPDATED FEATURE LIST
  # #################################################
  
  # Creating a unique list of nodes and mac addresses
  
  node_mac <- unique(CPEData[, c("node_name", "src_node_id")])
  node_mac <-  subset(node_mac, src_node_id %in% mac_new$src_node_id)
  
  
  # Shape-based distances - Dynamic Time Warping (DTW) Distance
  
  for (i in 1:nrow(node_mac))
  {
    
    node_series <- NA
    mac_snrdn_series  <- NA
    
    node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
    mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
    
    node_mac$snrdn_dtw[i] <- TSDistances(node_series, mac_snrdn_series , distance="dtw", sigma=10)
    
  }
  
  # Edit based distances - Longest Common Subsequence (LCSS) - SNR_DN
  
  for (i in 1:nrow(node_mac))
  {
    
    node_series <- NA
    mac_snrdn_series  <- NA
    
    node_series <- NodeData$snr_up_sma[NodeData$node_name == node_mac$node_name[i]]
    mac_snrdn_series <- mac_snrdn$snr_dn_sma[mac_snrdn$src_node_id == node_mac$src_node_id[i]]
    
    
    node_mac$snrdn_lcss[i] <- TSDistances(node_series, mac_snrdn_series , distance="lcss", epsilon=0.1)
    
  }
  
  # Feaure-based distances - Cross-correlation based - SNR_DN AND PATHLOSS
  
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
  
  
  ########################################################################
  ## INCULDING THE TIME SERIES DISTANCE METRICS IN THE UPDATE FEATURE LIST
  ########################################################################
  
  # Adding TS similarity columns to feature list
  
  
  mac_new$snrdn_dtw <- node_mac$snrdn_dtw[match(mac_new$src_node_id, node_mac$src_node_id)]
  
  mac_new$snrdn_lcss <- node_mac$snrdn_lcss[match(mac_new$src_node_id, node_mac$src_node_id)]
  
  mac_new$snrdn_ccor <- node_mac$snrdn_ccor[match(mac_new$src_node_id, node_mac$src_node_id)]
  
  mac_new$pathloss_ccor <- node_mac$pathloss_ccor[match(mac_new$src_node_id, node_mac$src_node_id)]
  
  
  # #################################################
  # ## FINAL IMPUTATION ON THE FEATURE LIST
  # #################################################
  
  # Replace all infiinite values by NA
  
  is.na(mac_new) <- do.call(cbind,lapply(mac_new, is.infinite))
  
  # Replace all NaN values by NA
  
  is.nan.data.frame <- function(x)
    do.call(cbind, lapply(x, is.nan))
  
  mac_new[is.nan.data.frame(mac_new)] <- NA
  
  # Impuation by median for columns with missing value 
  
  setDT(mac_new)
  
  mac_new[, snrdn_sd :=  replace(snrdn_sd, is.na(snrdn_sd), median(snrdn_sd, na.rm=TRUE)), by = node_name]
  mac_new[, pathloss_delta :=  replace(pathloss_delta, is.na(pathloss_delta), median(pathloss_delta, na.rm=TRUE)), by = node_name]
  mac_new[, pathloss_sd :=  replace(pathloss_sd, is.na(pathloss_sd), median(pathloss_sd, na.rm=TRUE)), by = node_name]
  
  mac_new[, snrdn_dtw :=  replace(snrdn_dtw, is.na(snrdn_dtw), median(snrdn_dtw, na.rm=TRUE)), by = node_name]
  mac_new[, snrdn_lcss :=  replace(snrdn_lcss, is.na(snrdn_lcss), median(snrdn_lcss, na.rm=TRUE)), by = node_name]
  mac_new[, snrdn_ccor :=  replace(snrdn_ccor, is.na(snrdn_ccor), median(snrdn_ccor, na.rm=TRUE)), by = node_name]
  mac_new[, pathloss_ccor :=  replace(pathloss_ccor, is.na(pathloss_ccor), median(pathloss_ccor, na.rm=TRUE)), by = node_name]
  
  
  mac_new <- as.data.frame(mac_new)
  
  
    return(mac_new)
  
}





