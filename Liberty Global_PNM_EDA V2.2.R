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

# Attributes for Node time series data 

summary(NodeData)

# Node length and NAs

node_len <- ddply(NodeData, .(node_name), nrow)
setnames(node_len, "V1", "node_len")

node_na <- aggregate(snr_up ~ node_name, data=NodeData, function(x) {sum(is.na(x))}, na.action = NULL)
setnames(node_na, "snr_up", "node_na_cnt")

node_len_na <- merge(x= node_len, y = node_na, by = "node_name", all.x = TRUE)

rm(node_len, node_na)

df <- melt(node_len_na, id.vars="node_name")

ggplot(df, aes(node_name,value, col=variable)) + 
  geom_point() + 
  geom_line(aes(group = variable))

# Distribution or descriptive statistics of snr_up per node

summary_snrup_node <- NodeData %>% group_by(node_name) %>% 
              dplyr::summarise(
              avg=mean(snr_up,na.rm = TRUE), 
              min=min(snr_up,na.rm = TRUE), 
              max=max(snr_up,na.rm = TRUE), 
              SD=sd(snr_up,na.rm = TRUE))

df <- melt(summary_snrup_node, id.vars='node_name')
ggplot(df, aes(x=node_name, y=value, color = variable, group = 1)) + geom_point() + facet_wrap(~variable)

# Distribution or descriptive statistics of snr_dn per mac address

summary_snrdn <-   CPEData_n %>% 
    group_by(src_node_id) %>% 
    dplyr::summarise(
    avg=mean(snr_dn,na.rm = TRUE), 
    min=min(snr_dn,na.rm = TRUE), 
    max=max(snr_dn,na.rm = TRUE), 
    SD=sd(snr_dn,na.rm = TRUE))


df <- melt(summary_snrdn, id.vars='src_node_id')
ggplot(df, aes(x=src_node_id, y=value, color = variable, group = 1)) + geom_point() + facet_wrap(~variable)


# Distribution of Length of time series per mac address

boxplot(mac_feature_list$mac_len, xlab = "Mac Series Length", col = "grey", ylim = c(250,1500), horizontal = TRUE)

# Distribution of total number of Mac address per node - Each mac address corresponds to only 1 node.

node_mac <- CPEData_n %>%
  group_by(node_name) %>%
  dplyr::summarise(count = n_distinct(src_node_id))

plot(node_mac$count, type = "l")

# Distrbution of snr_dn and pathloss NAs per mac address

ggplot(mac_feature_list, aes(x = perc_snrdn_na*100)) + geom_histogram(bins = 10, color = "red", fill = "gray")

ggplot(mac_feature_list, aes(x = perc_pathloss_na*100)) + geom_histogram(bins = 10, color = "red", fill = "gray")


# Distribution or descriptive statistics of pathloss per mac address

summary_pathloss <-   CPEData_n %>% 
  group_by(src_node_id) %>% 
  dplyr::summarise(
    avg=mean(pathloss,na.rm = TRUE), 
    min=min(pathloss,na.rm = TRUE), 
    max=max(pathloss,na.rm = TRUE), 
    SD=sd(pathloss,na.rm = TRUE))

df <- melt(summary_pathloss, id.vars='src_node_id')
ggplot(df, aes(x=src_node_id, y=value, color = variable, group = 1)) + geom_point() + facet_wrap(~variable)

####### Descriptive stat to compare node and mac time series ############

node_desc_stat <- setDT(NodeData)[ , list(node.len=NROW(hour_stamp), node.min=min(hour_stamp), node.max=max(hour_stamp), node.mean=mean(hour_stamp)), by= .(node_name)]

mac_desc_stat <- setDT(CPEData_n)[ , list(mac.len=NROW(hour_stamp), mac.min=min(hour_stamp), mac.max=max(hour_stamp), mac.mean=mean(hour_stamp)), by= .(node_name,src_node_id)]

desc_stat <- merge(x = node_desc_stat, y = mac_desc_stat, by = "node_name", all.y = TRUE)

rm(node_desc_stat, mac_desc_stat)

# Adding an TRUE/FALSE column in desc_stat to show if mac_len = node_len

desc_stat$Match <- desc_stat$node.len == desc_stat$mac.len


# Visuals for the features extracted

boxplot(mac_feature_list$snr_ratio, xlab = "snrdn to snrup ratio", horizontal = TRUE)

boxplot(mac_feature_list$pathloss_txpwrup_ratio, xlab = "pathloss_txpwrup_ratio", horizontal = TRUE)

boxplot(mac_feature_list$pathloss_rxpwrup_ratio, xlab = "pathloss_rxpwrup_ratio", horizontal = TRUE)

boxplot(mac_feature_list$snrdn_tdiff/3600, 
        xlab = "Average time diff in hours between successive non NA snr_dn values",
        horizontal = TRUE)

boxplot(mac_feature_list$pathloss_tdiff/3600, 
        xlab = "Average time diff in hours between successive non NA pathloss values",
        horizontal = TRUE)






