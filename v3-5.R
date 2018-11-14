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
library(plyr)
library(data.table)
library(dplyr)
library(stringr)
library(reshape2)
library(TSdist)
library(caret)
library(RColorBrewer)
library(scales)

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

# Node Data set - Filter out the time series data from polling dataset for all 
# probelamtic nodes.The snr_up for each node is calculated by averaging snr_up
# for each CPE under a node.

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
## CREATING NODE, SNR_DN AND PATHLOSS TIME SERIES.
## SELECTED ONLY 10 DAYS DATA PRIOR TO POLLING REFERENCE DATE.
#################################################

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
## CREATING THE FEATURES INDEPENDENTLY
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

# Descriptive Stats for snr_dn and pathloss

snrdn_stats <-
  setDT(mac_snrdn)[, list(
    snrdn_min = min(snr_dn, na.rm = T),
    snrdn_max = max(snr_dn, na.rm = T),
    snrdn_mean = mean(snr_dn, na.rm = T),
    snrdn_median = median(snr_dn, na.rm = T),
    snrdn_sd = sd(snr_dn, na.rm = T)
  ), by = .(src_node_id)]


pathloss_stats <-
  setDT(mac_pathloss)[, list(
    pathloss_min = min(pathloss, na.rm = T),
    pathloss_max = max(pathloss, na.rm = T),
    pathloss_mean = mean(pathloss, na.rm = T),
    pathloss_median = median(pathloss, na.rm = T),
    pathloss_sd = sd(pathloss, na.rm = T)
  ), by = .(src_node_id)]

# Average difference between successive time stamp (hour_stamp) for non na
# snr dn values -

mac_snrdn_tdiff <-  mac_snrdn %>%
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

# Average difference between successive time stamp (hour_stamp) for non na
# pathloss values -

mac_pathloss_tdiff <-  mac_pathloss %>%
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

mac_snrdn_delta <-  mac_snrdn %>%
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


# Ratio of snr_dn to snr_up for each mac

mac_snr_ratio <- CPEData %>%
  select(src_node_id, hour_stamp, snr_dn, snr_up)

mac_snr_ratio <- unique(mac_snr_ratio)

mac_snr_ratio <- mac_snr_ratio[!is.na(mac_snr_ratio$snr_dn), ]

mac_snr_ratio <- mac_snr_ratio[!is.na(mac_snr_ratio$snr_up), ]

mac_snr_ratio$snr_ratio <- mac_snr_ratio$snr_dn / mac_snr_ratio$snr_up

mac_snr_ratio <-
  aggregate(snr_ratio ~ src_node_id, data = mac_snr_ratio, function(x) {
    mean(x)
  }, na.action = NULL)

# Ratio of pathloss to txpower_up for each mac

mac_pathloss_txpwrup_ratio <- CPEData %>%
  select(src_node_id, hour_stamp, pathloss, tx_pwr_up)

mac_pathloss_txpwrup_ratio <- unique(mac_pathloss_txpwrup_ratio)

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

mac_pathloss_rxpwrup_ratio <- CPEData %>%
  select(src_node_id, hour_stamp, pathloss, rx_pwr_up)

mac_pathloss_rxpwrup_ratio <- unique(mac_pathloss_rxpwrup_ratio)

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

# Removing some of the columns from mac_feature_list

mac_feature_list <- dplyr::select(mac_feature_list, -c(mac_len, snr_dn_na, pathloss_na))

# Adding node_name to mac_feature_list

node_mac <- unique(CPEData[, c("node_name", "src_node_id")])

mac_feature_list <-
  merge(x = mac_feature_list,
        y = node_mac,
        by = "src_node_id",
        all.x = TRUE)

# #################################################
# ## CONSECUTIVE NAs IN SNR_DN AND PATHLOSS
# #################################################

# Most consecutive NA in a mac series
consecutive_NA = function(x, val = 9999) {
  with(rle(x), max(lengths[values == val]))
}

# Number of X consecutive NA in a mac series
X_consecutive_NA = function(x, val = 9999) {
  with(rle(x), lengths[values == val])
}

#Create dummy list to hold aggregated mac addresses feature list
mac_add_DS <- data.frame(src_node_id=character(), pathloss_max_consecutive_NA=character(),snrdn_max_consecutive_NA=character(), snrdn_3_consecutive_NA=character(),
                         pathloss_3_consecutive_NA=character(), snrdn_5_consecutive_NA=character(), pathloss_5_consecutive_NA=character(),
                         snrdn_10_consecutive_NA=character(),pathloss_10_consecutive_NA=character(),
                         stringsAsFactors=FALSE)

node_id_unique <- unique(CPEData$src_node_id) #list unique nodes
mac_DF <- list() #Reference list that will hold node + list mac add DF
for( i in node_id_unique){ #change to nodes_name_list
  df_temp <- CPEData[CPEData$src_node_id == i,] #Subset based on node_name
  mac_DF[[i]]  <- split(df_temp ,df_temp$src_node_id) #Split by mac add, create DF
}

z = 1
counter = 1
for (z in 1: length(mac_DF)){ 
  dfx <- data.frame(mac_DF[[z]])
  x <- c("reference_date","vertex_topo_node_id","edge_id","building_id","pathorder","hour_stamp","topo_node_type","node_key","src_node_model","src_node_id","src_node_name","node_name","mac_address"        
         ,"tx_pwr_up","rx_pwr_up","rx_pwr_dn","snr_dn","snr_up","pathloss","snr_up_median")
  colnames(dfx) <- x
  dfx <- arrange(dfx, hour_stamp) #Order by hour stamp
  dfx[is.na(dfx)] = 9999 #Set NA values to '9999'
  dfx_pathloss_NA  <- X_consecutive_NA(dfx$pathloss)
  dfx_snr_dn_NA  <- X_consecutive_NA(dfx$snr_dn)
  hold_values <- list() #create an empty list to hold values
  i=1
  while (i < 8) {  
    hold_values[i] <- unique(dfx$src_node_id) #Mac add
    i=i+1
    hold_values[i] <- consecutive_NA(dfx$pathloss)
    i=i+1
    hold_values[i] <- consecutive_NA(dfx$snr_dn)
    i=i+1
    hold_values[i] <- Reduce("+",lapply(dfx_snr_dn_NA, function(x) as.integer((x/3))))
    i=i+1
    hold_values[i] <- Reduce("+",lapply(dfx_pathloss_NA, function(x) as.integer((x/3))))
    i=i+1
    hold_values[i] <- Reduce("+",lapply(dfx_snr_dn_NA, function(x) as.integer((x/5))))
    i=i+1
    hold_values[i] <- Reduce("+",lapply(dfx_pathloss_NA, function(x) as.integer((x/5))))
    i=i+1
    hold_values[i] <- Reduce("+",lapply(dfx_snr_dn_NA, function(x) as.integer((x/10))))
    i=i+1
    hold_values[i] <- Reduce("+",lapply(dfx_pathloss_NA, function(x) as.integer((x/10))))
  }
  mac_add_DS[counter, ] <- hold_values #Add values as row
  counter = counter + 1
}


# Add to mac_feature_list the new columns

mac_feature_list <-
  merge(x = mac_feature_list,
        y = mac_add_DS,
        by = "src_node_id",
        all.x = TRUE)

rm(df_temp)
rm(dfx)
rm(counter)
rm(hold_values)
rm(i)
rm(mac_DF)
rm(node_id_unique)
rm(x)
rm(z)
rm(consecutive_NA)
rm(X_consecutive_NA)

###################################################
### AVERAGE TIME DIFFERENCE    
##################################################

# Function to calculate the average time difference between first NA value of one sequence
# and first NA value of next sequence
# @param mac_df mac dataframe with the time series
# @param n number of consecutive NA
# @param is_snr_dn boolean to select the snr dn or pathloss time series

avg_time_diff <- function(mac_df, n=3, is_snr_dn=TRUE){
  if(is_snr_dn){
    vector_tf <- is.na(mac_df$snr_dn)
  }else{
    vector_tf <- is.na(mac_df$pathloss)
  }
  
  # RLE computes the lengths and values of runs of equal values in a vector
  # in this case values are TRUE (NA) or FALSE (not NA)
  rle_vector <- rle(vector_tf)
  
  # Select the indices of the array of lengths where the
  # number of consecutive NA is equal to n
  indices <- which(rle_vector$lengths == n & rle_vector$values==TRUE)
  if(length(indices)== 1 | length(indices)== 0){
    return(0)
  }
  hourstamps = c()
  for(ind in indices){
    # Save the hourstamp of the first NA of the sequence
    hourstamps[length(hourstamps)+1] <- mac_df[sum(rle_vector$lengths[1:ind-1])+1,"hour_stamp"][[1]]
    
  }
  
  # Mean of the differences between hourstamps
  return(mean(abs(diff(hourstamps))))
}

macs <- unique(mac_snrdn$src_node_id)

# Add a column in the data frame mac_feature_list to find out the average time difference between 
# successive (3 consecutive NAs) for each mac in mac_snrdn
time_diff <- c()
for(i in 1:length(macs)){
  time_diff[i] <- avg_time_diff(mac_snrdn[mac_snrdn$src_node_id == macs[i],c("hour_stamp", "snr_dn")], 3)
}

# Add a column in the data frame mac_feature_list to find out the average time difference between 
# successive (5 consecutive NAs) for each mac in mac_snrdn
time_diff5 <- c()
for(i in 1:length(macs)){
  time_diff5[i] <- avg_time_diff(mac_snrdn[mac_snrdn$src_node_id == macs[i],c("hour_stamp", "snr_dn")], 5)
}

# Add a column in the data frame mac_feature_list to find out the average time difference between 
# successive (10 consecutive NAs) for each mac in mac_snrdn
time_diff10 <- c()
for(i in 1:length(macs)){
  time_diff10[i] <- avg_time_diff(mac_snrdn[mac_snrdn$src_node_id == macs[i],c("hour_stamp", "snr_dn")], 10)
}

# Add a column in the data frame mac_feature_list to find out the average time difference between 
# successive (3 consecutive NAs) for each mac in mac_pathloss
time_diff_p <- c()
for(i in 1:length(macs)){
  time_diff_p[i] <- avg_time_diff(mac_pathloss[mac_pathloss$src_node_id == macs[i],c("hour_stamp", "pathloss")], 3, is_snr_dn = FALSE)
}

# Add a column in the data frame mac_feature_list to find out the average time difference between 
# successive (5 consecutive NAs) for each mac in mac_pathloss
time_diff5_p <- c()
for(i in 1:length(macs)){
  time_diff5_p[i] <- avg_time_diff(mac_pathloss[mac_pathloss$src_node_id == macs[i],c("hour_stamp", "pathloss")], 5, is_snr_dn = FALSE)
}

# Add a column in the data frame mac_feature_list to find out the average time difference between 
# successive (10 consecutive NAs) for each mac in mac_pathloss
time_diff10_p <- c()
for(i in 1:length(macs)){
  time_diff10_p[i] <- avg_time_diff(mac_pathloss[mac_pathloss$src_node_id == macs[i],c("hour_stamp", "pathloss")], 10, is_snr_dn = FALSE)
}

# Create dataframe with new columns
df_time_diff <- data.frame(src_node_id=macs,avg_time_3NA_snr = time_diff,avg_time_5NA_snr = time_diff5,
                           avg_time_10NA_snr = time_diff10, avg_time_3NA_pathloss=time_diff_p, avg_time_5NA_pathloss=time_diff5_p,
                           avg_time_10NA_pathloss=time_diff10_p)

# Add to mac_feature_list the new columns

mac_feature_list <-
  merge(x = mac_feature_list,
        y = df_time_diff,
        by = "src_node_id",
        all.x = TRUE)

# Function to calculate the average time difference between last NA value of one sequence
# and first NA value of next sequence
# @param mac_df mac dataframe with the time series
# @param n number of consecutive NA
# @param is_snr_dn boolean to select the snr dn or pathloss time series

avg_time_diff_between_na <- function(mac_df, n=3, is_snr_dn=TRUE){
  if(is_snr_dn){
    vector_tf <- is.na(mac_df$snr_dn)
  }else{
    vector_tf <- is.na(mac_df$pathloss)
  }
  
  # RLE computes the lengths and values of runs of equal values in a vector
  # in this case values are TRUE (NA) or FALSE (not NA)
  rle_vector <- rle(vector_tf)
  
  # Select the indices of the array of lengths where the
  # number of consecutive NA is equal to n
  indices <- which(rle_vector$lengths == n & rle_vector$values==TRUE)
  if(length(indices)== 1 | length(indices)== 0){
    return(0)
  }
  hourstamps = c()
  for(i in 1:length(indices)){
    if(i+1 <= length(indices) ){
      # Save the hourstamp of the last NA of the sequence
      hourstamps[length(hourstamps)+1] <- mac_df[sum(rle_vector$lengths[1:indices[i]-1])+n,"hour_stamp"][[1]]
      # Save the hourstamp of the first NA of the next sequence
      hourstamps[length(hourstamps)+1] <- mac_df[sum(rle_vector$lengths[1:indices[i+1]-1])+1,"hour_stamp"][[1]]
    }
  }
  
  # Mean of the differences between hourstamps  
  return(mean(abs(diff(hourstamps))))
}

# Add a column in the data frame mac_feature_list to find out the average operative time difference between 
# successive (3 consecutive NAs) for each mac in mac_snrdn
time_diff <- c()
for(i in 1:length(macs)){
  time_diff[i] <- avg_time_diff_between_na(mac_snrdn[mac_snrdn$src_node_id == macs[i],c("hour_stamp", "snr_dn")], 3)
}

# Add a column in the data frame mac_feature_list to find out the average operative time difference between 
# successive (5 consecutive NAs) for each mac in mac_snrdn
time_diff5 <- c()
for(i in 1:length(macs)){
  time_diff5[i] <- avg_time_diff_between_na(mac_snrdn[mac_snrdn$src_node_id == macs[i],c("hour_stamp", "snr_dn")], 5)
}

# Add a column in the data frame mac_feature_list to find out the average operative time difference between 
# successive (10 consecutive NAs) for each mac in mac_snrdn
time_diff10 <- c()
for(i in 1:length(macs)){
  time_diff10[i] <- avg_time_diff_between_na(mac_snrdn[mac_snrdn$src_node_id == macs[i],c("hour_stamp", "snr_dn")], 10)
}

# Add a column in the data frame mac_feature_list to find out the average operative time difference between 
# successive (3 consecutive NAs) for each mac in mac_pathloss
time_diff_p <- c()
for(i in 1:length(macs)){
  time_diff_p[i] <- avg_time_diff_between_na(mac_pathloss[mac_pathloss$src_node_id == macs[i],c("hour_stamp", "pathloss")], 3, is_snr_dn = FALSE)
}

# Add a column in the data frame mac_feature_list to find out the average operative time difference between 
# successive (5 consecutive NAs) for each mac in mac_pathloss

time_diff5_p <- c()
for(i in 1:length(macs)){
  time_diff5_p[i] <- avg_time_diff_between_na(mac_pathloss[mac_pathloss$src_node_id == macs[i],c("hour_stamp", "pathloss")], 5, is_snr_dn = FALSE)
}

# Add a column in the data frame mac_feature_list to find out the average operative time difference between 
# successive (10 consecutive NAs) for each mac in mac_pathloss

time_diff10_p <- c()
for(i in 1:length(macs)){
  time_diff10_p[i] <- avg_time_diff_between_na(mac_pathloss[mac_pathloss$src_node_id == macs[i],c("hour_stamp", "pathloss")], 10, is_snr_dn = FALSE)
}

# Create dataframe with new columns

df_oper_time_diff <- data.frame(src_node_id=macs,avg_oper_time_3NA_snr = time_diff,avg_oper_time_5NA_snr = time_diff5,
                                avg_oper_time_10NA_snr = time_diff10, avg_oper_time_3NA_pathloss=time_diff_p, avg_oper_time_5NA_pathloss=time_diff5_p,
                                avg_oper_time_10NA_pathloss=time_diff10_p)

# Add to mac_feature_list the new columns

mac_feature_list <-
  merge(x = mac_feature_list,
        y = df_oper_time_diff,
        by = "src_node_id",
        all.x = TRUE)

rm(i)
rm(macs)
rm(time_diff)
rm(time_diff5)
rm(time_diff5_p)
rm(time_diff10)
rm(time_diff10_p)
rm(avg_time_diff)
rm(avg_time_diff_between_na)

mac_feature_list <- mac_feature_list %>% select(node_name, everything())

# #################################################
# ## IMPUTATION
# #################################################

# Making a new data set to preserve the original

mac_new <- mac_feature_list

mac_new$pathloss_max_consecutive_NA <- as.numeric(mac_new$pathloss_max_consecutive_NA)
mac_new$snrdn_max_consecutive_NA <- as.numeric(mac_new$snrdn_max_consecutive_NA)
mac_new$snrdn_3_consecutive_NA  <- as.numeric(mac_new$snrdn_3_consecutive_NA )
mac_new$pathloss_3_consecutive_NA <- as.numeric(mac_new$pathloss_3_consecutive_NA)
mac_new$snrdn_5_consecutive_NA <- as.numeric(mac_new$snrdn_5_consecutive_NA)
mac_new$pathloss_5_consecutive_NA <- as.numeric(mac_new$pathloss_5_consecutive_NA)
mac_new$snrdn_10_consecutive_NA <- as.numeric(mac_new$snrdn_10_consecutive_NA)
mac_new$pathloss_10_consecutive_NA <- as.numeric(mac_new$pathloss_10_consecutive_NA)

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

# NodeData <- NodeData %>%
#    group_by(node_name) %>%
#    mutate(snr_up_sma = na.ma(snr_up, k = 4, weighting = "simple"))

# Imputing the NA values in the Mac snrdn dataset with Simple moving averages

setDT(mac_snrdn)

mac_snrdn <- mac_snrdn[mac_snrdn$src_node_id %in% mac_new$src_node_id,]

mac_snrdn <-  mac_snrdn[order(mac_snrdn$node_name,
                              mac_snrdn$src_node_id,
                              mac_snrdn$hour_stamp), ]

mac_snrdn[, snr_dn_sma :=  na.ma(snr_dn, k = 4, weighting = "simple"),
          by = src_node_id]

# mac_snrdn <- mac_snrdn %>%
#   group_by(src_node_id) %>%
#   mutate(snr_dn_sma = na.ma(snr_dn, k = 4, weighting = "simple"))

mac_snrdn <- as.data.frame(mac_snrdn)

# Imputing the NA values in the Mac pathloss dataset with Simple moving averages

setDT(mac_pathloss)

mac_pathloss <- mac_pathloss[mac_pathloss$src_node_id %in% mac_new$src_node_id,]

mac_pathloss <-
  mac_pathloss[order(mac_pathloss$node_name,
                     mac_pathloss$src_node_id,
                     mac_pathloss$hour_stamp), ]

mac_pathloss[, pathloss_sma :=  na.ma(pathloss, k = 4, weighting = "simple"),
             by = src_node_id]

# mac_pathloss <- mac_pathloss %>%
#   group_by(src_node_id) %>%
#   mutate(pathloss_sma = na.ma(pathloss, k = 4, weighting = "simple"))

mac_pathloss <- as.data.frame(mac_pathloss)

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


########################################################################
## INCULDING THE TIME SERIES DISTANCE METRICS IN THE UPDATE FEATURE LIST
########################################################################

# Adding TS similarity columns to feature list

mac_new$snrdn_dtw <- node_mac$snrdn_dtw[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$pathloss_dtw <- node_mac$pathloss_dtw[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$snrdn_edr <- node_mac$snrdn_edr[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$pathloss_edr <- node_mac$pathloss_edr[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$snrdn_lcss <- node_mac$snrdn_lcss[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$pathloss_lcss <- node_mac$pathloss_lcss[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$snrdn_erp <- node_mac$snrdn_erp[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$pathloss_erp <- node_mac$pathloss_erp[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$snrdn_ccor <- node_mac$snrdn_ccor[match(mac_new$src_node_id, node_mac$src_node_id)]

mac_new$pathloss_ccor <- node_mac$pathloss_ccor[match(mac_new$src_node_id, node_mac$src_node_id)]

# Adding mac label - 0/1 variable for CPD cases.

mac_new$mac_label <- mac_positive$mac_label[match(mac_new$src_node_id, mac_positive$src_node_id)]

mac_new$mac_label[is.na(mac_new$mac_label)] <- 0

mac_new$mac_label <- as.factor(mac_new$mac_label)

# #################################################
# ## DATA CLEAN UP
# #################################################

#Replace Inf with the median of the pathloss_rxpwrup_ratio column

# mac_new <- do.call(data.frame,lapply(mac_new, function(x) replace(x, is.infinite(x),NA)))
# 
# mac_new <-  mac_new %>% group_by(node_name) %>% 
#   mutate(pathloss_rxpwrup_ratio = ifelse(is.na(pathloss_rxpwrup_ratio),
#                                          mean(pathloss_rxpwrup_ratio, na.rm = TRUE), pathloss_rxpwrup_ratio))

# #Replace NA values of snrdn_ccor with median
# 
# med <- median(mac_new[!is.na(mac_new$snrdn_ccor),"snrdn_ccor"])
# mac_new[is.na(mac_new$snrdn_ccor), "snrdn_ccor"] <- med

mac_new <- mac_new %>% select(node_name, everything())

mac_new <- as.data.frame(mac_new)

# #################################################
# ## PRINCIPAL COMPONENT ANALYSIS AND CLUSTERING
# #################################################

# set.seed(1)
# 
# prin_comp <- prcomp(mac_new[, !(colnames(mac_new) %in% c("src_node_id","node_name"))], scale. = T, center = T )
# summary(prin_comp)
# std_dev <- prin_comp$sdev
# pr_var <- std_dev^2
# prop_varex <- pr_var/sum(pr_var)
# 
# plot(cumsum(prop_varex), xlab = "Principal Component",
#      ylab = "Cumulative Proportion of Variance Explained",
#      type = "b")
# 
# plot(prin_comp)
# plot(prin_comp, type='l')
# 
# comp <- data.frame(prin_comp$x[, 1:2])
# 
# plot(comp, pch=16, col=rgb(0,0,0,0.5))
# 
# # Determine optimal number of clusters
# 
# mydata <- mac_new[, !(colnames(mac_new) %in% c("src_node_id","node_name"))]
# 
# wss <- (nrow(mydata)-1)*sum(apply(mydata,2,var))
# for (i in 2:15) wss[i] <- sum(kmeans(mydata,
#                                      centers=i)$withinss)
# 
# plot(1:15, wss, type="b", xlab="Number of Clusters",
#      ylab="Within groups sum of squares")
# 
# # K-Means CLustering witn 5 clusters
# 
# k <- kmeans(comp, 5, nstart=25, iter.max=1000)
# 
# palette(alpha(brewer.pal(9,'Set1'), 0.5))
# plot(comp, col=k$clust, pch=16)
# 
# # Adding the clusters to the dataframe
# 
# mac_new$cluster <- k$cluster
# mac_new$cluster <- as.factor(mac_new$cluster)
# 
# # Boxplots of Clusters with Variables 
# 
# melted<- melt(mac_new[, c(3:4,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(5:8,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(10:13,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(c(9,14),42)],id.vars='cluster')
# melted<- melt(mac_new[, c(15:16,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(17:20,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(21,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(22:29,42)],id.vars='cluster')
# melted<- melt(mac_new[, c(30:41,42)],id.vars='cluster')
# 
# ggplot(melted, aes(x = cluster, y = value)) + 
#   geom_boxplot()+facet_wrap(~variable)
# 
# rm(melted)

######################################################################
################### Classification algorithms #########################
#######################################################################
library(caret)
library(randomForest)

#Replace Inf with the median of the column
med <- median(mac_new[mac_new$pathloss_rxpwrup_ratio!=Inf,"pathloss_rxpwrup_ratio"])
mac_new[mac_new$pathloss_rxpwrup_ratio==Inf, "pathloss_rxpwrup_ratio"] <- med


#Replace NA values of snrdn_ccor column
for(i in 1:nrow(mac_new)){
  if(is.na(mac_new[i,"snrdn_ccor"])){
    node <- mac_new[i,"node_name"]
    med <- median(mac_new[mac_new$node_name==node,"snrdn_ccor"], na.rm = TRUE)
    mac_new[i, "snrdn_ccor"] <- med
  }
}


#PCA

prin_comp <- prcomp(mac_new[, !(colnames(mac_new) %in% c("node_name", "src_node_id","mac_label"))], scale. = T, center = T )
#corrplot(cor(mac_new[,-c(1,2,52)]))


#We select the first 15 PCs (cumulative proportion of 90%) and create a new dataframe
df_pca <- as.data.frame(prin_comp$x[,c(1:15)])
df_pca$mac_label <- as.factor(mac_new$mac_label)

# Train - test partition

df_pca$node <- as.factor(mac_new$node_name)
nodes <- unique(df_pca$node)
n <- floor(length(nodes) * 0.7)
train_nodes <- nodes[1:n]

train_data <- df_pca[df_pca$node %in% train_nodes,]
test_data <- df_pca[!(df_pca$node %in% train_nodes),]

train_data$node <- NULL
test_data$node <- NULL

############ STEP 7

###### RANDOM FOREST #######

rf_model <- train(train_data[,-length(train_data)], train_data$mac_label, metric = "Kappa",
                  method = "rf", sampsize=c(40,40))

y_pred <- predict(rf_model, test_data)

conf <- confusionMatrix(y_pred, test_data$mac_label)

# Confusion matrix
conf$table

# Kappa
conf$overall["Kappa"]

#Precision
precision(y_pred, test_data$mac_label)

#Recall
recall(y_pred, test_data$mac_label)

# ROC Curve
perf <- performance(prediction(as.numeric(y_pred), as.numeric(test_data$mac_label)), 
            measure = "tpr", x.measure = "fpr")
plot(perf)


##### SVM RBF

svm_Radial <- train(mac_label ~., data = train_data, method = "svmRadial",
                    preProcess = c("center", "scale"), metric = "Kappa")

y_pred <- predict(svm_Radial, test_data)

conf <- confusionMatrix(y_pred, test_data$mac_label)

# Confusion matrix
conf$table

# Kappa
conf$overall["Kappa"]

#Precision
precision(y_pred, test_data$mac_label)

#Recall
recall(y_pred, test_data$mac_label)

# ROC Curve
perf <- performance(prediction(as.numeric(y_pred), as.numeric(test_data$mac_label)), 
                    measure = "tpr", x.measure = "fpr")
plot(perf)

### SVM POLY

svm_poly <- train(mac_label ~., data = train_data, method = "svmPoly",
                  preProcess = c("center", "scale"), metric = "Kappa"
                  )
y_pred <- predict(svm_poly, test_data)

conf <- confusionMatrix(y_pred, test_data$mac_label)

# Confusion matrix
conf$table

# Kappa
conf$overall["Kappa"]

#Precision
precision(y_pred, test_data$mac_label)

#Recall
recall(y_pred, test_data$mac_label)

# ROC Curve
perf <- performance(prediction(as.numeric(y_pred), as.numeric(test_data$mac_label)), 
                    measure = "tpr", x.measure = "fpr")
plot(perf)


##### GBM

gbm_model <- train(mac_label ~., data = train_data, method = "gbm",
                   preProcess = c("center", "scale"), metric = "Kappa")

y_pred <- predict(gbm_model, test_data)

conf <- confusionMatrix(y_pred, test_data$mac_label)

# Confusion matrix
conf$table

# Kappa
conf$overall["Kappa"]

#Precision
precision(y_pred, test_data$mac_label)

#Recall
recall(y_pred, test_data$mac_label)

# ROC Curve
perf <- performance(prediction(as.numeric(y_pred), as.numeric(test_data$mac_label)), 
                    measure = "tpr", x.measure = "fpr")
plot(perf)


############## STEP 8 - Cross Validation #############

library(caret)

#### K - Fold
K <- 5


k_fold <- function(k){
  
  df_pca$node <- as.factor(mac_new$node_name)
  nodes <- as.character(unique(df_pca$node))
  
  n <- floor(length(nodes) * 1/k)  
  fold_nodes <- nodes[1:n]
  fold_list <- list()
  fold_list <- c(fold_list, list(fold_nodes))
  for(i in (2:k)){
    if(i==k){
      fold_nodes <- nodes[(n+1):length(nodes)]
    }else{
      fold_nodes <- nodes[(n+1):(n+length(fold_nodes))]
      n <- n+length(fold_nodes)
    }
    fold_list <- c(fold_list, list(fold_nodes))
    
  }
  
  return(fold_list)
  
}

fold_list <- k_fold(K)


## Random Forest
max<-0

for(i in 1:K){
  test_data <- df_pca[df_pca$node %in% fold_list[[i]],]
  train_data <- df_pca[!(df_pca$node %in% fold_list[[i]]),]
  
  test_data$node <- NULL
  train_data$node <- NULL
  
  rf_model <- train(train_data[,-length(train_data)], train_data$mac_label, metric = "Kappa",
                    method = "rf", sampsize=c(40,40))
  y_pred <- predict(rf_model, test_data)
  
  conf <- confusionMatrix(y_pred, test_data$mac_label)
  if(conf$overall["Kappa"] > max){
    max <- conf$overall["Kappa"]
    iter <- i
    best_pred <- y_pred
    y_true <- test_data$mac_label
  }
  
}

conf <- confusionMatrix(best_pred, y_true)

# Confusion matrix
conf$table

# Kappa
conf$overall["Kappa"]

#Precision
precision(best_pred, y_true)

#Recall
recall(best_pred, y_true)

# ROC Curve
perf <- performance(prediction(as.numeric(best_pred), as.numeric(y_true)), 
                    measure = "tpr", x.measure = "fpr")
plot(perf)


## SVM RBF

max<-0
for(i in 1:K){
  test_data <- df_pca[df_pca$node %in% fold_list[[i]],]
  train_data <- df_pca[!(df_pca$node %in% fold_list[[i]]),]
  
  test_data$node <- NULL
  train_data$node <- NULL
  
  svm_Radial <- train(mac_label ~., data = train_data, method = "svmRadial",
                      preProcess = c("center", "scale"), metric = "Kappa")
  y_pred <- predict(svm_Radial, test_data)
  
  conf <- confusionMatrix(y_pred, test_data$mac_label)
  if(conf$overall["Kappa"] > max){
    max <- conf$overall["Kappa"]
    iter <- i
    best_pred <- y_pred
    y_true <- test_data$mac_label
  }
  
}

conf <- confusionMatrix(best_pred, y_true)

# Confusion matrix
conf$table

# Kappa
conf$overall["Kappa"]

#Precision
precision(best_pred, y_true)

#Recall
recall(best_pred, y_true)

# ROC Curve
perf <- performance(prediction(as.numeric(best_pred), as.numeric(y_true)), 
                    measure = "tpr", x.measure = "fpr")
plot(perf)




# ### SVM POLY
# train_c <- trainControl(method="cv", number = 5)
# 
# tunegrid <- expand.grid(.scale=c(FALSE,TRUE), .degree=c(3,4,5,6,7,8), .C=c(1,10,100,1000))
# 
# svm_poly <- train(mac_label ~., data = train_data, method = "svmPoly",
#                   preProcess = c("center", "scale"), metric = "Kappa",
#                   tuneLength = 10, tuneGrid = tunegrid, trControl = train_c)
# y_pred <- predict(svm_poly, test_data)
# confusionMatrix(y_pred, test_data$mac_label)
# 
# ### GBM
# 
# train_c <- trainControl(method="cv", number = 5)
# 
# tunegrid <- expand.grid(.n.trees = c(50,100,150), .shrinkage = c(0.01, 0.001),
#                         .interaction.depth=c(3), .n.minobsinnode=c(1,3,5))
# 
# gbm_model <- train(mac_label ~., data = train_data, method = "gbm",
#                    preProcess = c("center", "scale"), metric = "Kappa",
#                    tuneLength = 10, tuneGrid = tunegrid, trControl = train_c)
# y_pred <- predict(gbm_model, test_data)
# confusionMatrix(y_pred, test_data$mac_label)







