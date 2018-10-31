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
  mutate(snr_up_sma = na.ma(snr_up, k = 4, weighting = "simple"))

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


######################### Time Series Similarity ######################

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

#################### Updating tne updated feature list again ################

# Adding TS similarity columns to feature list

mac_feature_list_updated$snrdn_dtw <- node_mac$snrdn_dtw[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$pathloss_dtw <- node_mac$pathloss_dtw[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$snrdn_edr <- node_mac$snrdn_edr[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$pathloss_edr <- node_mac$pathloss_edr[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$snrdn_lcss <- node_mac$snrdn_lcss[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$pathloss_lcss <- node_mac$pathloss_lcss[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$snrdn_erp <- node_mac$snrdn_erp[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$pathloss_erp <- node_mac$pathloss_erp[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$snrdn_ccor <- node_mac$snrdn_ccor[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

mac_feature_list_updated$pathloss_ccor <- node_mac$pathloss_ccor[match(mac_feature_list_updated$src_node_id, node_mac$src_node_id)]

# Adding mac label - 0/1 variable for CPD cases

mac_feature_list_updated$mac_label <- mac_positive$mac_label[match(mac_feature_list_updated$src_node_id, mac_positive$src_node_id)]

mac_feature_list_updated$mac_label[is.na(mac_feature_list_updated$mac_label)] <- 0

######################################################################
################### Classification algorithms #########################
#######################################################################
library(caret)
library(randomForest)

#Replace Inf with the median of the column
med <- median(mac_feature_list_updated[mac_feature_list_updated$pathloss_rxpwrup_ratio!=Inf,"pathloss_rxpwrup_ratio"])
mac_feature_list_updated[mac_feature_list_updated$pathloss_rxpwrup_ratio==Inf, "pathloss_rxpwrup_ratio"] <- med


#Replace NA values of snrdn_ccor
med <- median(mac_feature_list_updated[!is.na(mac_feature_list_updated$snrdn_ccor),"snrdn_ccor"])
mac_feature_list_updated[is.na(mac_feature_list_updated$snrdn_ccor), "snrdn_ccor"] <- med


#PCA

prin_comp <- prcomp(mac_feature_list_updated[, !(colnames(mac_feature_list_updated) %in% c("src_node_id","mac_label"))], scale. = T, center = T )

#summary(prin_comp)
#corrplot(cor(mac_feature_list_updated[,-c(1,34)]))

#We select the first 13 PCs and create a new dataframe
df_pca <- as.data.frame(prin_comp$x[,c(1:13)])
df_pca$mac_label <- as.factor(mac_feature_list_updated$mac_label)

# Train - test partition
train_rows <- createDataPartition(y=df_pca$mac_label, p=0.75, list = FALSE)
train_data <- df_pca[train_rows,]

test_data <- df_pca[-train_rows,]

############## Cross Validation #############

## Random Forest
train_c <- trainControl(method="cv", number = 5)

tunegrid <- expand.grid(.mtry= c(2:13))

rf_model <- train(train_data[,-length(train_data)], train_data$mac_label, metric = "Kappa",
                  method = "rf", tuneGrid = tunegrid, trControl = train_c, sampsize=c(40,40))

y_pred <- predict(rf_model, test_data)

confusionMatrix(y_pred, test_data$mac_label)

## SVM RBF
train_c <- trainControl(method="cv", number = 5)

tunegrid <- expand.grid(.sigma=c(0.001, 0.01, 0.1, 0.0001, 0.00001), .C=c(1,10,100,1000))

svm_Radial <- train(mac_label ~., data = train_data, method = "svmRadial",
                    preProcess = c("center", "scale"), metric = "Kappa",
                    tuneLength = 10, tuneGrid = tunegrid, trControl = train_c)
y_pred <- predict(svm_Radial, test_data)
confusionMatrix(y_pred, test_data$mac_label)

### SVM POLY
train_c <- trainControl(method="cv", number = 5)

tunegrid <- expand.grid(.scale=c(FALSE,TRUE), .degree=c(3,4,5,6,7,8), .C=c(1,10,100,1000))

svm_poly <- train(mac_label ~., data = train_data, method = "svmPoly",
                  preProcess = c("center", "scale"), metric = "Kappa",
                  tuneLength = 10, tuneGrid = tunegrid, trControl = train_c)
y_pred <- predict(svm_poly, test_data)
confusionMatrix(y_pred, test_data$mac_label)

### GBM

train_c <- trainControl(method="cv", number = 5)

tunegrid <- expand.grid(.n.trees = c(50,100,150), .shrinkage = c(0.01, 0.001),
                        .interaction.depth=c(3), .n.minobsinnode=c(1,3,5))

gbm_model <- train(mac_label ~., data = train_data, method = "gbm",
                   preProcess = c("center", "scale"), metric = "Kappa",
                   tuneLength = 10, tuneGrid = tunegrid, trControl = train_c)
y_pred <- predict(gbm_model, test_data)
confusionMatrix(y_pred, test_data$mac_label)

#####################################################################################
# BAYESIAN NETWORK DEVELOPMENT BY METE.
# 1. Package calls
library(bnlearn)

# 2. Creating the input dataframe for the algorithms.
BNData <- mac_feature_list_updated
class(BNData)

# Conversion to matrix & get rid of NA values.
BNData <- as.matrix(BNData)
BNData[is.nan(BNData)] <- 0
BNData <- as.data.frame(BNData)

# To convert all values to factors
BNData[] <- lapply(BNData, as.factor)
class(BNData)
str(BNData)

# levels Before Bucketing the dataset
sapply(BNData, nlevels)

# src_node_id                mac_len              snr_dn_na          perc_snrdn_na            pathloss_na 
# 5110                     38                    154                    496                    152 
# perc_pathloss_na              snrdn_min              snrdn_max             snrdn_mean           snrdn_median 
# 498                    183                    113                   4813                    156 
# snrdn_sd           pathloss_min           pathloss_max          pathloss_mean        pathloss_median 
# 4790                    839                    895                   5093                   1018 
# pathloss_sd            snrdn_tdiff         pathloss_tdiff            snrdn_delta         pathloss_delta 
# 5110                    676                    680                   1610                   3044 
# snr_ratio pathloss_txpwrup_ratio pathloss_rxpwrup_ratio              mac_label 
# 5102                   5086                   5050                      2 

# Bucketing operation
head(BNData)

# V1.src_node_id >> Unique identifiers, no need for bucketing.

# V2. mac length >> BUCKETTED from 38 levels to 10 levels.
BNData$mac_len <- as.numeric(BNData$mac_len)
Bins_V2 <- cut(BNData$mac_len, 10, include.lowest = TRUE, labels = FALSE)
Bins_V2 <- as.data.frame(Bins_V2)

# V3. snr_dn_na >> BUCKETTED from 154 levels to 10 levels.
BNData$snr_dn_na <- as.numeric(BNData$snr_dn_na)
Bins_V3 <- cut(BNData$snr_dn_na, 10, include.lowest = TRUE, labels = FALSE)
Bins_V3 <- as.data.frame(Bins_V3)

# V4. perc_snrdn_na >> BUCKETTED from 496 levels to 10 levels.
BNData$perc_snrdn_na <- as.numeric(BNData$perc_snrdn_na)
Bins_V4 <- cut(BNData$perc_snrdn_na, 10, include.lowest = TRUE, labels = FALSE)
Bins_V4 <- as.data.frame(Bins_V4)

# V5. pathloss_na >> BUCKETTED from 152 levels to 10 levels.
BNData$pathloss_na <- as.numeric(BNData$pathloss_na)
Bins_V5 <- cut(BNData$pathloss_na, 10, include.lowest = TRUE, labels = FALSE)
Bins_V5 <- as.data.frame(Bins_V5)

# V6. perc_pathloss_na >> BUCKETTED from 498 levels to 10 levels.
BNData$perc_pathloss_na <- as.numeric(BNData$perc_pathloss_na)
Bins_V6 <- cut(BNData$perc_pathloss_na, 10, include.lowest = TRUE, labels = FALSE)
Bins_V6 <- as.data.frame(Bins_V6)

# V7. snrdn_min >> BUCKETTED from 183 levels to 10 levels.
BNData$snrdn_min <- as.numeric(BNData$snrdn_min)
Bins_V7 <- cut(BNData$snrdn_min, 10, include.lowest = TRUE, labels = FALSE)
Bins_V7 <- as.data.frame(Bins_V7)

# V8. snrdn_max >> BUCKETTED from 113 levels to 10 levels.
BNData$snrdn_max <- as.numeric(BNData$snrdn_max)
Bins_V8 <- cut(BNData$snrdn_max, 10, include.lowest = TRUE, labels = FALSE)
Bins_V8 <- as.data.frame(Bins_V8)

# V9. snrdn_mean >> BUCKETTED from 4813 levels to 20 levels.
BNData$snrdn_mean <- as.numeric(BNData$snrdn_mean)
Bins_V9 <- cut(BNData$snrdn_mean, 20, include.lowest = TRUE, labels = FALSE)
Bins_V9 <- as.data.frame(Bins_V9)

# V10. snrdn_median >> BUCKETTED from 156 levels to 10 levels
BNData$snrdn_median <- as.numeric(BNData$snrdn_median)
Bins_V10 <- cut(BNData$snrdn_median, 10, include.lowest = TRUE, labels = FALSE)
Bins_V10 <- as.data.frame(Bins_V10)

# V11. snrdn_sd >> BUCKETTED from 4790 levels to 20 levels. 
BNData$snrdn_sd <- as.numeric(BNData$snrdn_sd)
Bins_V11 <- cut(BNData$snrdn_sd, 20, include.lowest = TRUE, labels = FALSE)
Bins_V11 <- as.data.frame(Bins_V11)

# V12. pathloss_min >> BUCKETTED from 839 levels to 10 levels. 
BNData$pathloss_min <- as.numeric(BNData$pathloss_min)
Bins_V12 <- cut(BNData$pathloss_min, 10, include.lowest = TRUE, labels = FALSE)
Bins_V12 <- as.data.frame(Bins_V12)

# V13. pathloss_max >> BUCKETTED from 895 levels to 10 levels.
BNData$pathloss_max <- as.numeric(BNData$pathloss_max)
Bins_V13 <- cut(BNData$pathloss_max, 10, include.lowest = TRUE, labels = FALSE)
Bins_V13 <- as.data.frame(Bins_V13)

# V14. pathloss_mean >> BUCKETTED from 5093 levels to 20 levels. 
BNData$pathloss_mean <- as.numeric(BNData$pathloss_mean)
Bins_V14 <- cut(BNData$pathloss_mean, 20, include.lowest = TRUE, labels = FALSE)
Bins_V14 <- as.data.frame(Bins_V14)

# V15. pathloss_median >> >> BUCKETTED from 1018 levels to 10 levels.
BNData$pathloss_median <- as.numeric(BNData$pathloss_median)
Bins_V15 <- cut(BNData$pathloss_median, 10, include.lowest = TRUE, labels = FALSE)
Bins_V15 <- as.data.frame(Bins_V15)

# V16. pathloss_sd >> BUCKETTED from 5110 levels to 20 levels.
BNData$pathloss_sd <- as.numeric(BNData$pathloss_sd)
Bins_V16 <- cut(BNData$pathloss_sd, 20, include.lowest = TRUE, labels = FALSE)
Bins_V16 <- as.data.frame(Bins_V16)

# V17. snrdn_tdiff >> BUCKETTED from 676 levels to 10 levels.
BNData$snrdn_tdiff <- as.numeric(BNData$snrdn_tdiff)
Bins_V17 <- cut(BNData$snrdn_tdiff, 10, include.lowest = TRUE, labels = FALSE)
Bins_V17 <- as.data.frame(Bins_V17)

# V18. pathloss_tdiff >> BUCKETTED from 680 levels to 10 levels.
BNData$pathloss_tdiff <- as.numeric(BNData$pathloss_tdiff)
Bins_V18 <- cut(BNData$pathloss_tdiff, 10, include.lowest = TRUE, labels = FALSE)
Bins_V18 <- as.data.frame(Bins_V18)

# V19. snrdn_delta >> BUCKETTED from 1610 levels to 10 levels.
BNData$snrdn_delta <- as.numeric(BNData$snrdn_delta)
Bins_V19 <- cut(BNData$snrdn_delta, 10, include.lowest = TRUE, labels = FALSE)
Bins_V19 <- as.data.frame(Bins_V19)

# V20. pathloss_delta >> BUCKETTED from 3044 levels to 20 levels.
BNData$pathloss_delta <- as.numeric(BNData$pathloss_delta)
Bins_V20 <- cut(BNData$pathloss_delta, 20, include.lowest = TRUE, labels = FALSE)
Bins_V20 <- as.data.frame(Bins_V20)

# V21. snr_ratio >> BUCKETTED from 5102 levels to 20 levels.
BNData$snr_ratio <- as.numeric(BNData$snr_ratio)
Bins_V21 <- cut(BNData$snr_ratio, 20, include.lowest = TRUE, labels = FALSE)
Bins_V21 <- as.data.frame(Bins_V21)

# V22. pathloss_txpwrup_ratio >> BUCKETTED from 5086 levels to 20 levels. 
BNData$pathloss_txpwrup_ratio<- as.numeric(BNData$pathloss_txpwrup_ratio)
Bins_V22 <- cut(BNData$pathloss_txpwrup_ratio, 20, include.lowest = TRUE, labels = FALSE)
Bins_V22 <- as.data.frame(Bins_V22)

# V23. pathloss_rxpwrup_ratio >> BUCKETTED from 5050 levels to 20 levels. 
BNData$pathloss_rxpwrup_ratio<- as.numeric(BNData$pathloss_rxpwrup_ratio)
Bins_V23 <- cut(BNData$pathloss_rxpwrup_ratio, 20, include.lowest = TRUE, labels = FALSE)
Bins_V23 <- as.data.frame(Bins_V23)

# V24. snrdn_dtw >> BUCKETTED from 4956 levels to 20 levels.
BNData$snrdn_dtw<- as.numeric(BNData$snrdn_dtw)
Bins_V24 <- cut(BNData$snrdn_dtw, 20, include.lowest = TRUE, labels = FALSE)
Bins_V24 <- as.data.frame(Bins_V24)

# V25. pathloss_dtw >> BUCKETTED from 5110 levels to 20 levels.
BNData$pathloss_dtw<- as.numeric(BNData$pathloss_dtw)
Bins_V25 <- cut(BNData$pathloss_dtw, 20, include.lowest = TRUE, labels = FALSE)
Bins_V25 <- as.data.frame(Bins_V25)

# V26. snrdn_edr >> BUCKETTED from 223 levels to 10 levels.
BNData$snrdn_edr<- as.numeric(BNData$snrdn_edr)
Bins_V26 <- cut(BNData$snrdn_edr, 10, include.lowest = TRUE, labels = FALSE)
Bins_V26 <- as.data.frame(Bins_V26)

# V27. pathloss_edr >> BUCKETTED from 208 levels to 10 levels.
BNData$pathloss_edr<- as.numeric(BNData$pathloss_edr)
Bins_V27 <- cut(BNData$pathloss_edr, 10, include.lowest = TRUE, labels = FALSE)
Bins_V27 <- as.data.frame(Bins_V27)

# V28. snrdn_lcss >> BUCKETTED from 148 levels to 10 levels.
BNData$snrdn_lcss<- as.numeric(BNData$snrdn_lcss)
Bins_V28 <- cut(BNData$snrdn_lcss, 10, include.lowest = TRUE, labels = FALSE)
Bins_V28 <- as.data.frame(Bins_V28)

# V29. pathloss_lcss >> BUCKETTED from 127 levels to 10 levels.
BNData$pathloss_lcss<- as.numeric(BNData$pathloss_lcss)
Bins_V29 <- cut(BNData$pathloss_lcss, 10, include.lowest = TRUE, labels = FALSE)
Bins_V29 <- as.data.frame(Bins_V29)

# V30. snrdn_erp >> BUCKETTED from 4944 levels to 20 levels.
BNData$snrdn_erp<- as.numeric(BNData$snrdn_erp)
Bins_V30 <- cut(BNData$snrdn_erp, 20, include.lowest = TRUE, labels = FALSE)
Bins_V30 <- as.data.frame(Bins_V30)

# V31. pathloss_erp >> BUCKETTED from 5110 levels to 20 levels. 
BNData$pathloss_erp<- as.numeric(BNData$pathloss_erp)
Bins_V31 <- cut(BNData$pathloss_erp, 20, include.lowest = TRUE, labels = FALSE)
Bins_V31 <- as.data.frame(Bins_V31)

# V32. snrdn_ccor >> BUCKETTED from 4910 levels to 20 levels. 
BNData$snrdn_ccor<- as.numeric(BNData$snrdn_ccor)
Bins_V32 <- cut(BNData$snrdn_ccor, 20, include.lowest = TRUE, labels = FALSE)
Bins_V32 <- as.data.frame(Bins_V32)

# V33. pathloss_ccor >> BUCKETTED from 5110 levels to 20 levels. 
BNData$pathloss_ccor<- as.numeric(BNData$pathloss_ccor)
Bins_V33 <- cut(BNData$pathloss_ccor, 20, include.lowest = TRUE, labels = FALSE)
Bins_V33 <- as.data.frame(Bins_V33)

# V34. mac_label >> takes 2 values, no need for bucketing.

###################################################################
# Aggregating the variables and generating the input dataset.

# V1.src_node_id
SRC_NODE_ID <- BNData$src_node_id
# V2. mac length 
MAC_LENGTH <- Bins_V2
# V3. snr_dn_na
SNR_DN_NA <- Bins_V3
# V4. perc_snrdn_na
PERC_SNRDN_VA <- Bins_V4
# V5. pathloss_na
PATHLOSS_NA <- Bins_V5
# V6. perc_pathloss_na
PERC_PATHLOSS_NA <- Bins_V6
# V7. snrdn_min
SNR_DN_MIN <- Bins_V7
# V8. snrdn_max
SNR_DN_MAX <- Bins_V8
# V9. snrdn_mean
SNR_DN_MEAN <- Bins_V9
# V10. snrdn_median
SNR_DN_MEDIAN <- Bins_V10
# V11. snrdn_sd
SNR_DN_SD <- Bins_V11
# V12. pathloss_min
PATHLOSS_MIN <- Bins_V12
# V13. pathloss_max
PATHLOSS_MAX <- Bins_V13
# V14. pathloss_mean
PATHLOSS_MEAN <- Bins_V14
# V15. pathloss_median
PATHLOSS_MEDIAN <- Bins_V15
# V16. pathloss_sd
PATHLOSS_SD <- Bins_V16
# V17. snrdn_tdiff
SNRDN_TDIFF <- Bins_V17
# V18. pathloss_tdiff
PATHLOSS_TDIFF <- Bins_V18
# V19. snrdn_delta
SNRDN_DELTA <- Bins_V19
# V20. pathloss_delta
PATHLOSS_DELTA <- Bins_V20
# V21. snr_ratio
SNR_RATIO <- Bins_V21
# V22. pathloss_txpwrup_ratio
PATHLOSS_TXPWRUP_RATIO <- Bins_V22
# V23. pathloss_rxpwrup_ratio
PATHLOSS_RXPWRUP_RATIO <- Bins_V23
# V24. snrdn_dtw
SNRDN_DTW <- Bins_V24
# V25. pathloss_dtw
PATHLOSS_DTW <- Bins_V25
# V26. snrdn_edr
SNRDN_EDR <- Bins_V26
# V27. pathloss_edr
PATHLOSS_EDR <- Bins_V27
# V28. snrdn_lcss
SNRDN_LCSS <- Bins_V28
# V29. pathloss_lcss
PATHLOSS_LCSS <- Bins_V29
# V30. snrdn_erp
SNRDN_ERP <- Bins_V30
# V31. pathloss_erp
PATHLOSS_ERP <- Bins_V31
# V32. snrdn_ccor
SNRDN_CCOR <- Bins_V32
# V33. pathloss_ccor
PATHLOSS_CCOR <- Bins_V33
# V34. mac_label
MAC_LABEL <- BNData$mac_label

BNInput <- cbind(SRC_NODE_ID,
                 MAC_LENGTH,
                 SNR_DN_NA,
                 PERC_SNRDN_VA,
                 PATHLOSS_NA,
                 PERC_PATHLOSS_NA,
                 SNR_DN_MIN,
                 SNR_DN_MAX,
                 SNR_DN_MEAN,
                 SNR_DN_MEDIAN,
                 SNR_DN_SD,
                 PATHLOSS_MIN,
                 PATHLOSS_MAX,
                 PATHLOSS_MEAN,
                 PATHLOSS_MEDIAN,
                 PATHLOSS_SD,
                 SNRDN_TDIFF,
                 PATHLOSS_TDIFF,
                 SNRDN_DELTA,
                 PATHLOSS_DELTA,
                 SNR_RATIO,
                 PATHLOSS_TXPWRUP_RATIO,
                 PATHLOSS_RXPWRUP_RATIO,
                 SNRDN_DTW,
                 PATHLOSS_DTW,
                 SNRDN_EDR,
                 PATHLOSS_EDR,
                 SNRDN_LCSS,
                 PATHLOSS_LCSS,
                 SNRDN_ERP,
                 PATHLOSS_ERP,
                 SNRDN_CCOR,
                 PATHLOSS_CCOR,
                 MAC_LABEL)

# Column naming
colnames(BNInput) <- c("SRC_NODE_ID",
                       "MAC_LENGTH",
                       "SNR_DN_NA",
                       "PERC_SNRDN_VA",
                       "PATHLOSS_NA",
                       "PERC_PATHLOSS_NA",
                       "SNR_DN_MIN",
                       "SNR_DN_MAX",
                       "SNR_DN_MEAN",
                       "SNR_DN_MEDIAN",
                       "SNR_DN_SD",
                       "PATHLOSS_MIN",
                       "PATHLOSS_MAX",
                       "PATHLOSS_MEAN",
                       "PATHLOSS_MEDIAN",
                       "PATHLOSS_SD",
                       "SNRDN_TDIFF",
                       "PATHLOSS_TDIFF",
                       "SNRDN_DELTA",
                       "PATHLOSS_DELTA",
                       "SNR_RATIO",
                       "PATHLOSS_TXPWRUP_RATIO",
                       "PATHLOSS_RXPWRUP_RATIO",
                       "SNRDN_DTW",
                       "PATHLOSS_DTW",
                       "SNRDN_EDR",
                       "PATHLOSS_EDR",
                       "SNRDN_LCSS",
                       "PATHLOSS_LCSS",
                       "SNRDN_ERP",
                       "PATHLOSS_ERP",
                       "SNRDN_CCOR",
                       "PATHLOSS_CCOR",
                       "MAC_LABEL")

colnames(BNInput)

# Dataframe factorized
BNInput %>% mutate_if(is.integer, funs(factor(.)))
BNInput %>% mutate_if(is.character, funs(factor(.)))

# To convert the integer values to factors
BNInput[] <- lapply(BNInput, as.factor)

# levels After Bucketing the dataset
sapply(BNInput, nlevels)

# SRC_NODE_ID             MAC_LENGTH              SNR_DN_NA          PERC_SNRDN_VA            PATHLOSS_NA       PERC_PATHLOSS_NA 
# 5110                     10                     10                     10                     10                     10 
# SNR_DN_MIN             SNR_DN_MAX            SNR_DN_MEAN          SNR_DN_MEDIAN              SNR_DN_SD           PATHLOSS_MIN 
# 10                     10                     20                     10                     20                     10 
# PATHLOSS_MAX          PATHLOSS_MEAN        PATHLOSS_MEDIAN            PATHLOSS_SD            SNRDN_TDIFF         PATHLOSS_TDIFF 
# 10                     20                     10                     20                     10                     10 
# SNRDN_DELTA         PATHLOSS_DELTA              SNR_RATIO PATHLOSS_TXPWRUP_RATIO PATHLOSS_RXPWRUP_RATIO              SNRDN_DTW 
# 10                     20                     20                     20                     20                     20 
# PATHLOSS_DTW              SNRDN_EDR           PATHLOSS_EDR             SNRDN_LCSS          PATHLOSS_LCSS              SNRDN_ERP 
# 20                     10                     10                     10                     10                     20 
# PATHLOSS_ERP             SNRDN_CCOR          PATHLOSS_CCOR              MAC_LABEL 
# 20                     20                     20                      2 

# 3. Development of networks using four different structure learning algorithms that are embedded in bnlearn package. (Constraint-based, Score-based, and Hybrid algorithms);
GS_Network = gs(BNInput)
HC_Network = hc(BNInput)
IAMB_Network = iamb(BNInput)
TABU_Network = tabu(BNInput)

# 4. Plotting the network graphs for visualization.
plot(GS_Network)
plot(HC_Network)
plot(IAMB_Network)
plot(TABU_Network)

# Fine Different layout representaitons for visual inspection;
# 4.1 dot
graphviz.plot(GS_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Grow-Shrink Algorithm Network, dot layout", sub = "Mac Features Dataset")
graphviz.plot(HC_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Hill-Climb Algorithm Network, dot layout", sub = "Mac Features Dataset")
graphviz.plot(IAMB_Network, highlight = NULL, layout = "dot", shape = "circle", main = "IAMB Algorithm Network, dot layout", sub = "Mac Features Dataset")
graphviz.plot(TABU_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Tabu Algorithm Network, dot layout", sub = "Mac Features Dataset")

# 4.2 neato
graphviz.plot(GS_Network, highlight = NULL, layout = "neato", shape = "circle", main = "Grow-Shrink Algorithm Network, neato layout", sub = "Mac Features Dataset")
graphviz.plot(HC_Network, highlight = NULL, layout = "neato", shape = "circle", main = "Hill-Climb Algorithm Network, neato layout", sub = "Mac Features Dataset")
graphviz.plot(IAMB_Network, highlight = NULL, layout = "neato", shape = "circle", main = "IAMB Algorithm Network, neato layout", sub = "Mac Features Dataset")
graphviz.plot(TABU_Network, highlight = NULL, layout = "neato", shape = "circle", main = "Tabu Algorithm Network, neato layout", sub = "Mac Features Dataset")

# 4.3 twopi
graphviz.plot(GS_Network, highlight = NULL, layout = "twopi", shape = "circle", main = "Grow-Shrink Algorithm Network, twopi layout", sub = "Mac Features Dataset")
graphviz.plot(HC_Network, highlight = NULL, layout = "twopi", shape = "circle", main = "Hill-Climb Algorithm Network, twopi layout", sub = "Mac Features Dataset")
graphviz.plot(IAMB_Network, highlight = NULL, layout = "twopi", shape = "circle", main = "IAMB Algorithm Network, twopi layout", sub = "Mac Features Dataset")
graphviz.plot(TABU_Network, highlight = NULL, layout = "twopi", shape = "circle", main = "Tabu Algorithm Network, twopi layout", sub = "Mac Features Dataset")

# 4.4 circo
graphviz.plot(GS_Network, highlight = NULL, layout = "circo", shape = "circle", main = "Grow-Shrink Algorithm Network, circo layout", sub = "Mac Features Dataset")
graphviz.plot(HC_Network, highlight = NULL, layout = "circo", shape = "circle", main = "Hill-Climb Algorithm Network, circo layout", sub = "Mac Features Dataset")
graphviz.plot(IAMB_Network, highlight = NULL, layout = "circo", shape = "circle", main = "IAMB Algorithm Network, circo layout", sub = "Mac Features Dataset")
graphviz.plot(TABU_Network, highlight = NULL, layout = "circo", shape = "circle", main = "Tabu Algorithm Network, circo layout", sub = "Mac Features Dataset")

# 4.5 fdp
graphviz.plot(GS_Network, highlight = NULL, layout = "fdp", shape = "circle", main = "Grow-Shrink Algorithm Network, fdp layout", sub = "Mac Features Dataset")
graphviz.plot(HC_Network, highlight = NULL, layout = "fdp", shape = "circle", main = "Hill-Climb Algorithm Network, fdp layout", sub = "Mac Features Dataset")
graphviz.plot(IAMB_Network, highlight = NULL, layout = "fdp", shape = "circle", main = "IAMB Algorithm Network, fdp layout", sub = "Mac Features Dataset")
graphviz.plot(TABU_Network, highlight = NULL, layout = "fdp", shape = "circle", main = "Tabu Algorithm Network, fdp layout", sub = "Mac Features Dataset")

# 5. Printing graphs to a pdf.
# pdf("Liberty Global - Bayesian Network Graphs for MAC Features.pdf")
# 
# # Content of the pdf starts here.
# 
# <CONTENT OF THE PDF HERE>
# 
# # End writing to pdf.
# dev.off()