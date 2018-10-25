# Development of Common Path Distortion (CPD) Algorithm for Liberty Global.

# Project (Coding) Begin Date: 08/10/2018.

# Authors: Mete Sevinc, Subroto Mallick, Alejandra, Claudia, Hassan.

# Last Update / Modification: 23/10/2018.
# Latest version: v1.9.

# Modules;
# Module 1: Ben's code for establishing a database connection.
# Module 2: Time Series Analyses using Nodes and MAC Addresses datasets.
# Module 3: Exploratory Data Analyses, basic statistics & histograms.
# Module 4: Bayesian Network Development and printing the resulting graphs to pdf.
# Module 5: Evaluation of NA imputation methods

# Notes / Comments / Logs;
# Log 1; 17/10/2018: Some EDA functions from the young graduates were added.
# Log 2; 17/10/2018: Average SNR dataset can be obtained.
# Log 3; 18/10/2019: EDA Analyses can be performed.
# Log 4; 21/10/2018: Bayesian Network coding was initiated.
# Log 5; 22/10/2018: Added the latest version of time series code from Alejandra and Claudia.
# Log 6; 22/10/2018: Added the code to obtain the Nodes and MAC addresses time series datasets.
# Log 7; 22/10/2018: Added Time-series plotting code from Hassan.
# Log 8; 22/10/2018: BNs can be produced as a result of Module 4 and printed on a pdf file.
# Log 9; 24/10/2018: Updated time series functions and added the code to evaluate NA imputation methods.
# Log 10; 25/10/2018: Updated time series NA imputation code. Updated pollingTest_CPE database query.

# To-do List;
# Next tasks in line:
#  1. PCA Analyses. (Hassan)
#  2. Bayesian network Analyses. (Mete)
#  3. Imputation & time series Analyses. (Alejandra & Claudia)

######################################################################################################
##### --- Module 1. Ben's code for database connection --- ###########################################
######################################################################################################

# Use local library
.libPaths("/app/RPackageLibrary/R-3.3")

# Package Calls
library(RPostgreSQL)

# Create DB connection to localhost
drv <- RPostgreSQL::PostgreSQL()
con <- DBI::dbConnect(drv, dbname = 'PNM_CH_SA', user = 'postgres', host = '127.0.0.1', port = 5432, password = 'postgres')

# Show tables
tables <- DBI::dbListTables(con)
print(tables)

# Get Polling Data
pollingTest <- DBI::dbGetQuery(con,"SELECT * FROM TEST_CASES.test_cases_polling_data LIMIT 10;")
print(pollingTest)

# Reference Date
referenceTest <- DBI::dbGetQuery(con, "SELECT * FROM TEST_CASES.TEST_CASES_REFERENCE_DATA;")

# Create Query to get polling data for specific test case
queryPollingData <- function(testCaseRef, connection = con){
  require(glue)
  
  nodeName <- testCaseRef$node_name
  query = glue("SELECT *
               FROM TEST_CASES.test_cases_polling_data
               WHERE NODE_NAME='{nodeName}' AND reference_date = '{testCaseRef$polling_reference_date}'")
  queryResult = dbGetQuery(connection, query)
  return(as.data.frame(queryResult))
}

testCasePolling <- queryPollingData(referenceTest[4,])

###### Libraries #######

# Already installed;
#library(RPostgreSQL) # For database connection.
#library(plyr)
#library(tidyverse)
#library(DataExplorer) # for easy EDA
#library(dlookr)
#library(dplyr) # for data manipulation
#library(ggplot2) # for plotting
#library(mise)
#library(TSdist) # for time series similarity analyses

######################################################################################################
##### --- Module 2. Time Series Analyses --- #########################################################
######################################################################################################

# Create dataframe with node name, hour stamp and average SNR_UP.
node_data <- DBI::dbGetQuery(con,"SELECT ref.node_name, hour_stamp, AVG(snr_up) 
                             FROM TEST_CASES.test_cases_polling_data poll, TEST_CASES.TEST_CASES_REFERENCE_DATA ref 
                             WHERE poll.node_name=ref.node_name AND poll.topo_node_type='CPE'
                             GROUP BY ref.node_name, hour_stamp;")

# Create dataframe containig all CPE information.
pollingTest_CPE <- DBI::dbGetQuery(con,"SELECT DISTINCT poll.* FROM TEST_CASES.test_cases_polling_data poll, TEST_CASES.TEST_CASES_REFERENCE_DATA ref
                                   WHERE poll.topo_node_type='CPE' and poll.node_name=ref.node_name;")

# Create a dataframe containing all mac addresses time series information.
mac_data <- pollingTest_CPE[,c("node_name", "mac_address","hour_stamp", "snr_dn", "pathloss")]

# SNR Averages dataset query alternative (from Hassan);
# NodeData1 <- DBI::dbGetQuery(con,"SELECT DISTINCT a.node_name, a.hour_stamp, AVG(a.snr_up)
#                             FROM TEST_CASES.test_cases_polling_data a, TEST_CASES.TEST_CASES_REFERENCE_DATA b
#                             WHERE a.topo_node_type = 'CPE' and a.node_name = b.node_name
#                             GROUP by a.node_name, a.hour_stamp ORDER by a.node_name, a.hour_stamp; ")

# -------------------------------Functions to obtain the reference nodes-----

# A. Obtain the reference nodes
# @return array of unique reference node names
get_reference_nodes <- function(){
  return(unique(node_data$node_name))
}

# B. Obtain all mac addresses connected to a node
# @param node_name: node name from which we want to obtain the mac addresses
# @return array of unique mac addresses
node_get_macaddress <- function(node_name){
  return(unique(pollingTest_CPE$mac_address[pollingTest_CPE$node_name == node_name]))
}

# C. Obtain time series of a mac address
# @param mac_address: MAC address from which we want to obtain the time series
# @return dataframe with hour stamp, SNR down and pathloss values
mac_get_time_series <- function(mac_address){
  return(pollingTest_CPE[pollingTest_CPE$mac_address == mac_address,c("hour_stamp","snr_dn", "pathloss")])
}

# D. Obtain time series of a node
# @param node_name: node name from which we want to obtain the time series
# @return dataframe with hour stamp and average SNR up values
node_get_time_series <- function(node_name){
  return(node_data[node_data$node_name == node_name,c("hour_stamp","avg")])
}

# E. Obtain time series of a node as a ts object
# @param node_name: node name from which we want to obtain the time series
# @return ts with hour stamp and average SNR up values
node_get_time_series_ts <- function(node_name){
  return(as.ts(node_data[node_data$node_name == node_name,c("hour_stamp","avg")]))
} 

# --------Code for time series plot (used for presentation / Visualization)-----------------------------------

# Obtain time series for random node and a few random macs
ts = node_get_time_series("1636N01")
df_mac <- mac_get_time_series("54FA3E3712E1")
df_mac2 <- mac_get_time_series("905C44E7F3D6")
df_mac3 <- mac_get_time_series("905C4418A0CC")
df_mac4 <- mac_get_time_series("3C6200779B8C")

# Node 1636N01 and MAC address 54FA3E3712E1
a <- ggplot() + geom_line(data=ts, aes(x=hour_stamp, y=avg, colour="Node average upstream SNR")) + 
  geom_line(data=df_mac, aes(x=hour_stamp, y=snr_dn, colour="MAC address No. 1  downstream SNR")) +
  geom_line(data=df_mac, aes(x=hour_stamp, y=pathloss, colour="MAC address No. 1 pathloss"))+
  labs(title="Comparing node and MAC address No. 1 time series") + 
  xlab("Time") + ylab("dB") + theme_bw() + theme(legend.title=element_blank(), legend.position="bottom")

# Node 1636N01 and MAC address 905C44E7F3D6
b <- ggplot() + geom_line(data=ts, aes(x=hour_stamp, y=avg, colour="Node average upstream SNR")) + 
  geom_line(data=df_mac2, aes(x=hour_stamp, y=snr_dn, colour="MAC address No. 2  downstream SNR")) +
  geom_line(data=df_mac2, aes(x=hour_stamp, y=pathloss, colour="MAC address No. 2 pathloss"))+
  labs(title="Comparing node and MAC address No. 2 time series") + 
  xlab("Time") + ylab("dB") + theme_bw() + theme(legend.title=element_blank(), legend.position="bottom")

# Node 1636N01 and MAC address 905C4418A0CC
c <- ggplot() + geom_line(data=ts, aes(x=hour_stamp, y=avg, colour="Node average upstream SNR")) + 
  geom_line(data=df_mac3, aes(x=hour_stamp, y=snr_dn, colour="MAC address No. 3  downstream SNR")) +
  geom_line(data=df_mac3, aes(x=hour_stamp, y=pathloss, colour="MAC address No. 3 pathloss"))+
  labs(title="Comparing node and MAC address No. 3 time series") + 
  xlab("Time") + ylab("dB") + theme_bw() + theme(legend.title=element_blank(), legend.position="bottom")

# Node 1636N01 and MAC address 3C6200779B8C
d <- ggplot() + geom_line(data=ts, aes(x=hour_stamp, y=avg, colour="Node average upstream SNR")) + 
  geom_line(data=df_mac4, aes(x=hour_stamp, y=snr_dn, colour="MAC address No. 4  downstream SNR")) +
  geom_line(data=df_mac4, aes(x=hour_stamp, y=pathloss, colour="MAC address No. 4 pathloss"))+
  labs(title="Comparing node and MAC address No. 4 time series") + 
  xlab("Time") + ylab("dB") + theme_bw() + theme(legend.title=element_blank(), legend.position="bottom")

#----------- Code for mac addresses table (used for presentation) --------------

#Get the dataframe of the particular node and the selected columns
df_table <- pollingTest_CPE[pollingTest_CPE$node_name=="1636N01",c("node_name", "mac_address", "hour_stamp", "pathloss", "snr_dn")]

#Remove the empty strings mac addresses
df_table <- df_table[df_table$mac_address!="",]

#Get all the variables
min_hs <- aggregate(hour_stamp~mac_address, df_table, min)
names(min_hs) <- c("mac_address", "min_hour_stamp")

max_hs <- aggregate(hour_stamp~mac_address, df_table, max)
names(max_hs) <- c("mac_address", "max_hour_stamp")

min_snrdn <- aggregate(snr_dn~mac_address, df_table, min)
names(min_snrdn) <- c("mac_address", "min_snr_dn")

max_snrdn <- aggregate(snr_dn~mac_address, df_table, max)
names(max_snrdn) <- c("mac_address", "max_snr_dn")

median_snrdn <- aggregate(snr_dn~mac_address, df_table, median)
names(median_snrdn) <- c("mac_address", "median_snr_dn")

sd_snrdn <- aggregate(snr_dn~mac_address, df_table, sd)
names(sd_snrdn) <- c("mac_address", "sd_snr_dn")

min_pathloss <- aggregate(pathloss~mac_address, df_table, min)
names(min_pathloss) <- c("mac_address", "min_pathloss")

max_pathloss <- aggregate(pathloss~mac_address, df_table, max)
names(max_pathloss) <- c("mac_address", "max_pathloss")

median_pathloss <- aggregate(pathloss~mac_address, df_table, median)
names(median_pathloss) <- c("mac_address", "median_pathloss")

sd_pathloss <- aggregate(pathloss~mac_address, df_table, sd)
names(sd_pathloss) <- c("mac_address", "sd_pathloss")

# Merge all the dataframes by mac address
final_dataf <- Reduce(function(x,y) merge(x,y, by="mac_address", all=TRUE), list(min_hs, max_hs, min_snrdn, max_snrdn, median_snrdn, sd_snrdn,
                                                                                 min_pathloss, max_pathloss, median_pathloss, sd_pathloss))
#View(final_dataf)

# ----------------------------------------------------------------

# Time Series Plots (from Hassan)

#Get Polling Data
NodeData <- DBI::dbGetQuery(con,"SELECT DISTINCT a.node_name, a.hour_stamp, AVG(a.snr_up) 
                            FROM TEST_CASES.test_cases_polling_data a, TEST_CASES.TEST_CASES_REFERENCE_DATA b 
                            WHERE a.topo_node_type = 'CPE' and a.node_name = b.node_name 
                            GROUP by a.node_name, a.hour_stamp ORDER by a.node_name, a.hour_stamp; ")

library(plyr)
#Time series per node
df <- ddply(NodeData, .(node_name), nrow) 
colnames(df) <- c("node_name", "occurence")
#Na time series per node
df_na <- aggregate(avg ~ node_name, data=NodeData, function(x) {sum(is.na(x))}, na.action = NULL) 
colnames(df_na) <- c("node_name", "na")
#Combbine Time series and NA's
df_combi <- merge(df, df_na, by=0, all=TRUE) 
df_combi$Row.names = NULL
df_combi$node_name.y = NULL

df2 <- melt(df_combi, id.vars='node_name.x')
#Time seires per node
ggplot(df2, aes(x = reorder(node_name.x, -value), y=value, fill=variable)) +
  geom_bar(stat='identity', position='dodge') +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))

#Summary Statistics of avg snr up per node name 
library(dplyr)
NodeData %>% group_by(node_name) %>% 
  summarise(
    avrg=mean(avg,na.rm = TRUE), 
    min=min(avg,na.rm = TRUE), 
    max=max(avg,na.rm = TRUE), 
    SD=sd(avg,na.rm = TRUE))
node_summary1 <- copy(node_summary)
node_summary1$SD <- NULL
df3 <- melt(node_summary1, id.vars='node_name')
#Plot summary stats
ggplot(df3, aes(x = reorder(node_name, -value), y=value, fill=variable)) +
  geom_bar(stat='identity', position='dodge') +
  theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
  labs(x = "Node name")


######################################################################################################
##### --- Module 3. Exploratory Data Analyses --- ####################################################
######################################################################################################

# Summaries
summary(testCasePolling)
summary(pollingTest)
summary(referenceTest)

# 3.1 Report the first few lines of the datasets.
head(referenceTest)
head(pollingTest)
head(testCasePolling)

# 3.2 Datasets in visuals
plot_str(testCasePolling)
plot_str(pollingTest)
plot_str(referenceTest)

# 3.3 Missing values
plot_missing(testCasePolling)
plot_missing(pollingTest)
plot_missing(referenceTest)

# 3.3 Continuous variables (Histogram & Density plots)
plot_histogram(testCasePolling)
plot_histogram(pollingTest)
plot_histogram(referenceTest)

plot_density(testCasePolling)
plot_density(pollingTest)
plot_density(referenceTest)

# 3.4 Barplots
plot_bar(testCasePolling)
plot_bar(pollingTest)
plot_bar(referenceTest)

# 3.5 Create report
create_report(testCasePolling)
create_report(pollingTest)
create_report(referenceTest)

# YOUNG GRADUATES FINDINGS
# From Alejandra & Claudia

# TestCasePolling Dataset Analysis

# 1.	In order to find potential problematic buildings due to bad location, weather conditions, etc. we search those where the snr_up_median is below 25. When this happens, it means there would be dropped connections, packet loss or slow transfers.

table(testCasePolling$building_id[testCasePolling$snr_up_median<25])

# We found out that there are 51 out of 76 which meet this condition.  
# Now that we know that there could be problematic buildings, we want to calculate the actual percentage of instances (not including NA values) per building that meets the condition of snr_up_median being below 25:

table_bl_less_25<-table(testCasePolling$building_id[testCasePolling$snr_up_median<25])
count_bl<- count(testCasePolling$building_id[!is.na(testCasePolling$snr_up_median)])
table_bl_less_25<-as.data.frame(table_bl_less_25)
rm("bl_pct_less_25")
bl_pct_less_25 <- data.frame(building_id=integer(),percentage_less_25=double())
for( elem in table_bl_less_25$Var1){
  if(elem %in% count_bl$x){
    val <- table_bl_less_25$Freq[table_bl_less_25$Var1==elem]/count_bl$freq[count_bl$x==elem]
    bl_pct_less_25[nrow(bl_pct_less_25)+1, ] <- c(elem,val*100)
  }else{
    paste(elem,"not in count_bl")
  }
}

# 2. We now want to see the snr_up distribution in terms of the type of network device.

boxplot(snr_up~factor(topo_node_type), data=testCasePolling)

# We only find values for CPEs.
# 83% of the snr_up values are NA.

# 3.	We want to know if there are some models more potential to fail than others, so we sum the snr_up_median values that are less than 25 per model of network device.
sort(table(testCasePolling$src_node_model[testCasePolling$snr_up_median<25]), decreasing=TRUE) 

# Furthermore, we want to see the distribution of the variable snr_up_median for each device model.

boxplot(snr_up_median~src_node_model, data=testCasePolling)

# The model which stands out is MEDIABOX HD PHILIPS DCR7101/03. It has only 81 instances where snr_up_median has values different from NA (out of 1482 instances), and 45 of those are below 25.

# 4.	Are there some arcs where fails tend to occur more?

count(sort(table(testCasePolling$edge_id[testCasePolling$snr_up_median<25]),decreasing=TRUE))

# 174 out of 478 edges have snr_up_median values below 25. We still don't know if this will be helpful for the problem of estimating the CPD location, but we see common vertices in some of the edges.

# 5.	According to internet research (https://www.speedguide.net/faq/what-cable-modem-signal-levels-are-considered-good-78), "upstream power lower than 40dB may start introducing some packet loss (especially if you have much noise on the line)"

sort(table(testCasePolling$building_id[testCasePolling$tx_pwr_up<40]), decreasing=TRUE)

# We have searched for the buildings which present this problem, but this could be done with other variables.

# 6.	We also found that "If you hit 58 the modem will likely drop the connection and resync."

sort(table(testCasePolling$building_id[testCasePolling$tx_pwr_up>50]), decreasing=TRUE)

# Just like in the previous point, this is done with building IDs but it could be done with other variables as well.

# ----------------Variable-based investigation on testCasePolling dataset ------------------
# List of variables in testCasePolling

# 1. reference_date. <String variable>
mean(testCasePolling$reference_date)

# 2. vertex_topo_node_id <String variable>
mean(testCasePolling$vertex_topo_node_id)

# 3. edge_id <String variable>
mean(testCasePolling$edge_id)

# 4. building_id <Integer variable>
mean(testCasePolling$building_id)

# 5. pathorder <Integer variable>
mean(testCasePolling$pathorder)

# 6. hour_stamp <Date stamp>
mean(testCasePolling$hour_stamp)

# 7. topo_node_type / String variable
topo_node_types = table(testCasePolling$topo_node_type)
topo_node_types
plot(topo_node_types)

# 8. node_key <String variable>
mean(testCasePolling$node_key)

# 9. src_node_model / String variable
src_node_model = table(testCasePolling$src_node_model)
src_node_model
plot(src_node_model)

# 10. src_node_id <String variable>
mean(testCasePolling$src_node_id)

# 11. src_node_name <String variable>
mean(testCasePolling$src_node_name)

# 12. node_name <String variable>
mean(testCasePolling$node_name)

# 13. mac_address <String variable>
mean(testCasePolling$mac_address)

# 14. tx_pwr_up / <Numerical variable>
mean(testCasePolling$tx_pwr_up)
median(testCasePolling$tx_pwr_up)
plot(testCasePolling$tx_pwr_up)

# 15. rx_pwr_up / <Numerical variable>
mean(testCasePolling$rx_pwr_up)
median(testCasePolling$rx_pwr_up)
plot(testCasePolling$rx_pwr_up)

# 16. rx_pwr_dn / <Numerical variable>
mean(testCasePolling$rx_pwr_dn)
median(testCasePolling$rx_pwr_dn)
plot(testCasePolling$rx_pwr_dn)

# 17. snr_dn / <Numerical variable>
mean(testCasePolling$snr_dn)
median(testCasePolling$snr_dn)
plot(testCasePolling$snr_dn)

# 18. snr_up / <Numerical variable>
mean(testCasePolling$snr_up)
median(testCasePolling$snr_up)
plot(testCasePolling$snr_up)

# 19. pathloss
mean(testCasePolling$pathloss)

# 20. snr_up_median / <Numerical variable>
mean(testCasePolling$snr_up_median)
median(testCasePolling$snr_up_median)
plot(testCasePolling$snr_up_median)

# -------------------------------Reporting on the datasets------------------------------

# Report the dimensions of the input datasets.
dim(testCasePolling)
dim(referenceTest)
dim(pollingTest)

# Report the structure of the input datasets.
str(testCasePolling)
str(referenceTest)
str(pollingTest)

# Report the column sums for each variable.
# colSums(sapply(testCasePolling, !is.na))

# Report the summary of numeric values and structure of the data.
# summary(testCasePolling[,.SD, .SDcols =numeric_var])

# Report the percentage of data missing.
# sum(is.na(testCasePolling)) / (nrow(testCasePolling) *ncol(testCasePolling))

# Report the number of duplicated rows.
# cat("The number of duplicated rows are", nrow(testCasePolling) - nrow(unique(testCasePolling)))

# Get Polling Data
pollingTest_CPE <- DBI::dbGetQuery(con,"SELECT * FROM TEST_CASES.test_cases_polling_data
                                   WHERE topo_node_type='CPE';")

# Distribution or descriptive statistics of snr_dn per mac address
snr_dist <- aggregate(snr_dn ~ mac_address, pollingTest_CPE, mean)
snr_dist <- snr_dist[snr_dist$mac_address!="",]
summary(snr_dist$snr_dn)
hist(snr_dist$snr_dn, xlim=c(15,45), xlab="Average snr_dn", main="Histogram of average snr_dn per mac address")

#Distribution or descriptive statistics of pathloss per mac address
pathloss_dist <- aggregate(pathloss ~ mac_address, pollingTest_CPE, mean)
pathloss_dist <- pathloss_dist[pathloss_dist$mac_address!="",]
summary(pathloss_dist$pathloss)
hist(pathloss_dist$pathloss, xlim=c(10,60), xlab="Average pathloss", main="Histogram of average pathloss per mac address")

##################################################################################################################
##### --- Module 4. Bayesian Network Development --- #############################################################
##################################################################################################################

library(bnlearn) # The package to develop Bayesian Networks and make use of the utilities, methods and the structure learning / Scoring Algorithms
#library(ggplot2)
#library(graphics)
#library(graph)
#library(igraph)
#library(lattice) # for plotting the bn.fit instances
#library(Rgraphviz)
#library(gRain)

# To Read & Write Excel files
library(readxl)
library(xlsx)

# Supplementary packages
#library(deal) ## Used for rgraphviz
#library(pcalg) ## Used for rgraphviz
#library(stringr) ## Pipe
#library(BiocInstaller) ## Advance Graphics
#library(BayesianNetwork) 
#library(datasets) ## Advance manipulation of Datasets
#library(gRbase)   ## part of Shiny
#library(networkD3) ## part of Shiny
#library(dplyr)
#library(Magrittr)
#library(ROCR)
#library(Rmpfr)
#library(gmp)

Nodes_dataframe <- read.csv(file="Inputs/Toy_Nodes_Dataset.csv", header = TRUE)
MACs_dataframe <- read.csv(file="Inputs/Toy_MACs_Dataset.csv", header = TRUE)

# 4.1 DATA CLEANING & PRE-PROCESSING
# Convert mac_address from integer to numeric.
MACs_dataframe$mac_address <- as.numeric(MACs_dataframe$mac_address)

# 4.2 OBTAINING THE NETWORKS FROM EMBEDDED ALGORITHMS
# Algorithms for learning structure from the dataset (Constraint-based, Score-based, and Hybrid algorithms);

# 4.2.1. Grow-shrink algorithm
Nodes_gs_Network = gs(Nodes_dataframe)
MACs_gs_Network = gs(MACs_dataframe)

# 4.2.2. Hill climb (a.k.a Greedy search) algorithm
Nodes_hc_Network = hc(Nodes_dataframe)
MACs_hc_Network = hc(MACs_dataframe)

# 4.2.3. Fast incremental association algorithm
Nodes_iamb_Network = iamb(Nodes_dataframe)
MACs_iamb_Network = iamb(MACs_dataframe)

# 4.2.4 Tabu network
Nodes_tabu_Network = tabu(Nodes_dataframe)
MACs_tabu_Network = tabu(MACs_dataframe)

# 4.3 BASIC PLOTTING OF THE BNs

# 4.3.1. Plotting the Grow-shrink algorithm network
plot(Nodes_gs_Network)
plot(MACs_gs_Network)

# 4.3.2. plotting the Hill-climb algorithm network
plot(Nodes_hc_Network)
plot(MACs_hc_Network)

# 4.3.3. plotting the IAMB algorithm network
plot(Nodes_iamb_Network)
plot(MACs_iamb_Network)

# 4.3.4. plotting the Tabu algorithm results
plot(Nodes_tabu_Network)
plot(MACs_tabu_Network)

# 4.4 PRINTING GRAPHS TO A PDF

# Begin writing to pdf.
pdf("Liberty Global - Bayesian Network Graphs for Time Series Analyses.pdf")

# 4.4.1 Draw the networks derived from grow shrink algorithm
graphviz.plot(Nodes_gs_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Grow-Shrink Algorithm Network, Nodes Time Series", sub = NULL)
graphviz.plot(MACs_gs_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Grow-Shrink Algorithm Network, MAC Addressses Time Series", sub = NULL)

# 4.4.2 Draw the network derived from hill climb algorithm
graphviz.plot(Nodes_hc_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Hill-Climb Algorithm Network, Nodes Time Series", sub = NULL)
graphviz.plot(MACs_hc_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Hill-Climb Algorithm Network, MAC Addresses Time Series", sub = NULL)

# 4.4.3 Draw the network derived from iamb algorithm
graphviz.plot(Nodes_iamb_Network, highlight = NULL, layout = "dot", shape = "circle", main = "IAMB Algorithm Network, Nodes Time Series", sub = NULL)
graphviz.plot(MACs_iamb_Network, highlight = NULL, layout = "dot", shape = "circle", main = "IAMB Algorithm Network, MAC Addresses Time Series", sub = NULL)

# 4.4.4 Draw the network derived from tabu algorithm
graphviz.plot(Nodes_iamb_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Tabu Algorithm Network, Nodes Time Series", sub = NULL)
graphviz.plot(MACs_iamb_Network, highlight = NULL, layout = "dot", shape = "circle", main = "Tabu Algorithm Network, MAC Addresses Time Series", sub = NULL)

# End writing to pdf.
dev.off()

######################################################################################################
##### --- Module 5. Evaluation of NA imputation methods --- ##########################################
######################################################################################################
library(zoo)
library(imputeTS)

# Small example to know if we can decompose time series to obtain trend and seasonality
# Get time series from node
ts = node_get_time_series("1029N01")
ts_n1 <- ts(ts$avg, start = 1, end = length(ts$hour_stamp))

# Trend? Seasonality?
decompose(ts_n1) # error: time series has no or less than 2 periods

# -------------------------

# 1. Metrics for NA imputation
# A. RMSE
rmse <- function(actual, predicted){
  sqrt(mean((actual - predicted)^2))
}

# B. MAPE
mape <- function(actual, predicted){
  return (mean(abs((actual - predicted)/actual))*100)
}

# C. Perform several NA imputation methods on a group of ids and evaluate their predictions using RMSE and MAPE
# Methods: na.locf, na.ma, na.approx, na.aggregate and na.spline.
# @param ids: ids of devices to perform NA imputation metrics on
# @param isNode: boolean variable. If TRUE, param ids is node names. If FALSE, it is mac addresses
# @param is_snr_dn: boolean variable. If TRUE, imputaion is done in snr_dn time series. If FALSE, it is done in pathloss.
# @return dataframe with the average scores for each node and method 
summary_na_imputation <- function(ids, isNode=TRUE, is_snr_dn=TRUE){
  summary_df <- data.frame(id=character(), imputation=character(), avg_mape=double(), avg_rmse=double())
  
  for(n in ids){
    
    if(isNode==FALSE){
      # Get the time series for each mac address
      if(is_snr_dn){
        id_ts <- mac_get_time_series(n)$snr_dn
      }else{
        id_ts <- mac_get_time_series(n)$pathloss
      }
      
    }
    else {
      # Get the time series of each node
      id_ts <- node_get_time_series(n)$avg
    }
    
    # na.locf needs a TS with at least one numerical value
    # so any TS with all NA will be skipped
    if(sum(is.na(id_ts))== length(id_ts)){
      next
    # na.ma needs a TS with at least two numerical values
    # so any TS with less than two numerical values will be skipped      
    }else if(sum(is.na(id_ts)) > length(id_ts)-2){
      next
    }
    
    filled_ts <- na.aggregate(id_ts)
    
    # Perform the imputation methods
    sts_locf <- na.locf(id_ts)
    sts_approx <-na.approx(id_ts)
    sts_spline <-na.spline(id_ts)
    sts_aggre <- na.aggregate(id_ts)
    sts_ma <- na.ma(id_ts)
    
    # Run the RMSE and MAPE metrics for each imputation method
    rmse_locf <- rmse(filled_ts, sts_locf)
    mape_locf <- mape(filled_ts, sts_locf)
    rmse_approx <- rmse(filled_ts, sts_approx)
    mape_approx <- mape(filled_ts, sts_approx)
    rmse_spline <- rmse(filled_ts, sts_spline)
    mape_spline <- mape(filled_ts, sts_spline)
    rmse_aggre <- rmse(filled_ts, sts_aggre)
    mape_aggre <- mape(filled_ts, sts_aggre)
    rmse_ma <- rmse(filled_ts, sts_ma)
    mape_ma <- mape(filled_ts, sts_ma)
    
    # We add the average values in a data frame
    summary_df <- rbind(summary_df, data.frame(id_name=n, imputation="na.locf", avg_mape=mape_locf, avg_rmse=rmse_locf))
    summary_df <- rbind(summary_df, data.frame(id_name=n, imputation="na.approx", avg_mape=mape_approx, avg_rmse=rmse_approx))
    summary_df <- rbind(summary_df, data.frame(id_name=n, imputation="na.spline", avg_mape=mape_spline, avg_rmse=rmse_spline))
    summary_df <- rbind(summary_df, data.frame(id_name=n, imputation="na.aggregate", avg_mape=mape_aggre, avg_rmse=rmse_aggre))
    summary_df <- rbind(summary_df, data.frame(id_name=n, imputation="na.ma", avg_mape=mape_ma, avg_rmse=rmse_ma))
  }
  return(summary_df) 
  
}

# ---------------------------- NODE TS IMPUTATION EVALUATION - SNR_UP ---------------
# 2. Perform NA imputation methods on all the reference nodes 
df_summ <- summary_na_imputation(get_reference_nodes())

# Obtain the average scores for the nodes
# 2.1 Obtain the average score of the MAPE and RMSE for the method na.aggregate
df_agg <- df_summ[df_summ$imputation=="na.aggregate",]
avg_mape_agg <- mean(df_agg$avg_mape)
avg_rmse_agg <- mean(df_agg$avg_rmse)

# 2.2 Obtain the average score of the MAPE and RMSE for the method na.locf
df_locf <- df_summ[df_summ$imputation=="na.locf",]
avg_mape_locf <- mean(df_locf$avg_mape)
avg_rmse_locf <- mean(df_locf$avg_rmse)

# 2.3 Obtain the average score of the MAPE and RMSE for the method na.approx
df_approx <- df_summ[df_summ$imputation=="na.approx",]
avg_mape_approx <- mean(df_approx$avg_mape)
avg_rmse_approx <- mean(df_approx$avg_rmse)

# 2.4 Obtain the average score of the MAPE and RMSE for the method na.spline
df_spline <- df_summ[df_summ$imputation=="na.spline",]
avg_mape_spline <- mean(df_spline$avg_mape)
avg_rmse_spline <- mean(df_spline$avg_rmse)

# 2.5 Obtain the average score of the MAPE and RMSE for the method na.ma
df_ma <- df_summ[df_summ$imputation=="na.ma",]
avg_mape_ma <- mean(df_ma$avg_mape)
avg_rmse_ma <- mean(df_ma$avg_rmse)

# 2.6 Create evaluation dataframe 
df_scores <- data.frame(avg_metric=character(), method=character(), snr_up=double(), snr_dn=double(), pathloss=double()) 
df_scores <- rbind(df_scores, data.frame(avg_metric=c("MAPE", "MAPE", "MAPE", "MAPE", "MAPE", "RMSE", "RMSE", "RMSE", "RMSE", "RMSE")))
df_scores <- cbind(df_scores, data.frame(method=c("aggregate", "approx", "locf", "ma", "spline", "aggregate", "approx", "locf", "ma", "spline")))
df_scores <- cbind(df_scores, data.frame(snr_up=c(avg_mape_agg, avg_mape_approx, avg_mape_locf, avg_mape_ma, avg_mape_spline,
                                                  avg_rmse_agg, avg_rmse_approx, avg_rmse_locf, avg_rmse_ma, avg_rmse_spline)))


# ------------------------- MAC TS IMPUTATION EVALUATION - SNR_DN --------------------
# 3. Obtain the average scores for the mac series

# E. Obtain all the mac addresses for the reference nodes
# @return array of unique mac addresses for the reference nodes
get_all_macs <- function(){
  return (unique(mac_data[,"mac_address"]))
}

# Get all the mac addreses for the reference nodes
list_macs <- get_all_macs()
# Remove the NA macs (macs = "")
list_macs <- Filter(function(x)x!="", list_macs)

#Evaluate the NA imputation for the mac addresses regarding the snr_dn variable
df_summ_mac_snr <- summary_na_imputation(list_macs, isNode=FALSE)

# 3.1 Obtain the average score of the MAPE and RMSE for the method na.aggregate
df_agg <- df_summ_mac_snr[df_summ_mac_snr$imputation=="na.aggregate",]
avg_mape_agg <- mean(df_agg$avg_mape)
avg_rmse_agg <- mean(df_agg$avg_rmse)

# 3.2 Obtain the average score of the MAPE and RMSE for the method na.locf
df_locf <- df_summ_mac_snr[df_summ_mac_snr$imputation=="na.locf",]
avg_mape_locf <- mean(df_locf$avg_mape)
avg_rmse_locf <- mean(df_locf$avg_rmse)

# 3.3 Obtain the average score of the MAPE and RMSE for the method na.approx
df_approx <- df_summ_mac_snr[df_summ_mac_snr$imputation=="na.approx",]
avg_mape_approx <- mean(df_approx$avg_mape)
avg_rmse_approx <- mean(df_approx$avg_rmse)

# 3.4 Obtain the average score of the MAPE and RMSE for the method na.spline
df_spline <- df_summ_mac_snr[df_summ_mac_snr$imputation=="na.spline",]
avg_mape_spline <- mean(df_spline$avg_mape)
avg_rmse_spline <- mean(df_spline$avg_rmse)

# 3.5 Obtain the average score of the MAPE and RMSE for the method na.ma
df_ma <- df_summ_mac_snr[df_summ_mac_snr$imputation=="na.ma",]
avg_mape_ma <- mean(df_ma$avg_mape)
avg_rmse_ma <- mean(df_ma$avg_rmse)

# 3.6 Add scores for the snr_dn imputation
df_scores <- cbind(df_scores, data.frame(snr_dn=c(avg_mape_agg, avg_mape_approx, avg_mape_locf, avg_mape_ma, avg_mape_spline,
                                                  avg_rmse_agg, avg_rmse_approx, avg_rmse_locf, avg_rmse_ma, avg_rmse_spline)))

# ---------------------- MAC TS IMPUTATION EVALUATION - PATHLOSS  -------------------
# 4. Evaluate the NA imputation for the mac addresses regarding the pathloss variable
df_summ_mac_pathloss <- summary_na_imputation(list_macs, isNode=FALSE, is_snr_dn = FALSE)

# 4.1 Obtain the average score of the MAPE and RMSE for the method na.aggregate
df_agg <- df_summ_mac_pathloss[df_summ_mac_pathloss$imputation=="na.aggregate",]
avg_mape_agg <- mean(df_agg$avg_mape)
avg_rmse_agg <- mean(df_agg$avg_rmse)

# 4.2 Obtain the average score of the MAPE and RMSE for the method na.locf
df_locf <- df_summ_mac_pathloss[df_summ_mac_pathloss$imputation=="na.locf",]
avg_mape_locf <- mean(df_locf$avg_mape)
avg_rmse_locf <- mean(df_locf$avg_rmse)

# 4.3 Obtain the average score of the MAPE and RMSE for the method na.approx
df_approx <- df_summ_mac_pathloss[df_summ_mac_pathloss$imputation=="na.approx",]
avg_mape_approx <- mean(df_approx$avg_mape)
avg_rmse_approx <- mean(df_approx$avg_rmse)

# 4.4 Obtain the average score of the MAPE and RMSE for the method na.spline
df_spline <- df_summ_mac_pathloss[df_summ_mac_pathloss$imputation=="na.spline",]
avg_mape_spline <- mean(df_spline$avg_mape)
avg_rmse_spline <- mean(df_spline$avg_rmse)

# 4.5 Obtain the average score of the MAPE and RMSE for the method na.ma
df_ma <- df_summ_mac_pathloss[df_summ_mac_pathloss$imputation=="na.ma",]
avg_mape_ma <- mean(df_ma$avg_mape)
avg_rmse_ma <- mean(df_ma$avg_rmse)

# 4.6 Add scores for the pathloss imputation
df_scores <- cbind(df_scores, data.frame(pathloss=c(avg_mape_agg, avg_mape_approx, avg_mape_locf, avg_mape_ma, avg_mape_spline,
                                                    avg_rmse_agg, avg_rmse_approx, avg_rmse_locf, avg_rmse_ma, avg_rmse_spline)))

              
                    
# ------- NA IMPUTATION FOR NODE TIME SERIES --------

node_ts <- node_data[node_data$node_name!="", c("node_name", "hour_stamp","avg")]
node_ts <- node_ts %>%
  group_by(node_name) %>%
  mutate(avg_snr_up=if(sum(!is.na(avg)) >=2) na.ma(avg) else NA)

node_ts <- node_ts[!is.na(node_ts$avg),]
node_ts$avg <- NULL

# ------- NA IMPUTATION FOR MAC ADDRESSES TIME SERIES --------

mac_ts <- mac_data[mac_data$mac_address!="", c("mac_address", "hour_stamp","snr_dn", "pathloss")]
mac_ts <- mac_ts %>%
      group_by(mac_address) %>%
       mutate(snr_dn_ma_expo=if(sum(!is.na(snr_dn)) >=2) na.ma(snr_dn) else NA)

mac_ts <- mac_ts[!is.na(mac_ts$snr_dn),]

mac_ts <- mac_ts %>%
  group_by(mac_address) %>%
  mutate(pathloss_ma_expo=if(sum(!is.na(pathloss)) >=2) na.ma(pathloss) else NA)

mac_ts <- mac_ts[!is.na(mac_ts$pathloss),]
mac_ts$snr_dn <- NULL
mac_ts$pathloss <- NULL
                    
                    
######################################################################################################
##### --- Module 6. Principal Component Analysis & Clustering --- ####################################
######################################################################################################
# A. Getting Data into correct format for PCA
nodes_names_list <- unique(NodeData$node_name) #list unique nodes
#node_s_1029N01 <- NodeData[NodeData$node_name == nodes_names_list[1], ]
#node_s_1029N01 <- node_s_1029N01[order(node_s_1029N01$hour_stamp),]
temp_nodes <- c("1029N01","1071N04", "8037N04", "3661N06", "94000N03")

mylist <- list() #Reference list that will hold node + list mac add DF
for( i in temp_nodes){ #change to nodes_name_list
  df_temp <- CPEData[CPEData$node_name == i,] #Subset based on node_name
  mylist[[i]]  <- split(df_temp ,df_temp$src_node_id) #Split by mac add, create DF
}

#Create dummy list to hold aggregated mac addresses features for individual node
node_x_mac_add <- data.frame(src_node_id=rep(NA, N), NA_Percentage=rep("", N), sd_snr_dn=rep("", N), sd_tx_pwr_up=rep("", N),
                            mean_pathloss=rep("", N),sd_rx_pwr_up=rep("", N),sd_snr_up_median=rep("", N),mean_snr_dn=rep("", N),        
                            mean_tx_pwr_up=rep("", N),sd_pathloss=rep("", N),mean_rx_pwr_up=rep("", N),mean_rx_pwr_dn=rep("", N),
                            mean_snr_up_median=rep("", N),sub_snr_up_tx_up=rep("", N),sub_snr_up_rx_up=rep("", N),sub_snr_dn_rx_dn=rep("", N),
                            div_snr_up_tx_up=rep("", N),div_snr_up_rx_up=rep("", N),div_snr_dn_rx_dn=rep("", N),div_rx_pwr_up_snr_dn=rep("", N),
                            # as many cols as you need
                            stringsAsFactors=FALSE)

#node_x_mac_add <- data.frame(src_node_id=rep("", N), NA_Percentage=rep("", N), sd_snr_dn=rep("", N),sd_pathloss=rep("", N),mean_snr_dn=rep("", N),        
#                                   mean_pathloss=rep("", N),mean_rx_pwr_dn=rep("", N),sub_snr_dn_rx_dn=rep("", N),
#                                   div_snr_dn_rx_dn=rep("", N),
                                   # as many cols as you need
#                                   stringsAsFactors=FALSE)

z = 1
for (z in 1: length(mylist[['8037N04']])){ #z in number of mac address in node
  dfx <- mylist[['8037N04']][[z]] #'1071N04' node of z mac add DF in list
  hold_values <- list() #create an empty list to hold values
  i=1
  while (i < 20) {  #Loops through 20 times to create features by aggregating DF
    #tx_pwr_up, rx_pwr_up , rx_pwr_dn, snr_dn, snr_up, pathloss, snr_up_median = 7
    #Creating extra features, 21 created..
    hold_values[i] <- unique(dfx$src_node_id) #Mac add
    i=i+1
    hold_values[i] <- sum(is.na(dfx))/(count(dfx) * 7)  #NA percentage
    i=i+1
    hold_values[i] <- sd(dfx$snr_dn, na.rm = TRUE)
    i=i+1
    hold_values[i] <- sd(dfx$tx_pwr_up, na.rm = TRUE)
    i=i+1
    hold_values[i] <- sd(dfx$pathloss, na.rm = TRUE)
    i=i+1
    hold_values[i] <- sd(dfx$rx_pwr_up, na.rm = TRUE) 
    i=i+1
    hold_values[i] <- sd(dfx$snr_up_median, na.rm = TRUE)
    i-i+1
    hold_values[i] <- mean(dfx$snr_dn, na.rm = TRUE) #Mean of snr_dn
    i=i+1
    hold_values[i] <- mean(dfx$tx_pwr_up, na.rm = TRUE) #Mean of tx_pwr_up 
    i=i+1
    hold_values[i] <- mean(dfx$pathloss, na.rm = TRUE) #Mean of pathloss
    i=i+1
    hold_values[i] <- mean(dfx$rx_pwr_up, na.rm = TRUE) #Mean of rx_pwr_up
    i=i+1
    hold_values[i] <- mean(dfx$rx_pwr_dn, na.rm = TRUE) #Mean of rx_pwr_dn
    i=i+1
    hold_values[i] <- mean(dfx$snr_up_median, na.rm = TRUE) #Mean of snr_up_median
    i=i+1
    hold_values[i] <- mean(dfx$snr_up - dfx$tx_pwr_up, na.rm = TRUE)
    i=i+1
    hold_values[i] <- mean(dfx$snr_up - dfx$rx_pwr_up, na.rm = TRUE)
    i=i+1
    hold_values[i] <- mean(dfx$snr_dn - dfx$rx_pwr_dn, na.rm = TRUE)
    i=i+1
    hold_values[i] <- mean(dfx$snr_up / dfx$tx_pwr_up, na.rm = TRUE) 
    i=i+1
    hold_values[i] <- mean(dfx$snr_up / dfx$rx_pwr_up, na.rm = TRUE) 
    i=i+1
    hold_values[i] <- as.numeric(mean(dfx$snr_dn / dfx$rx_pwr_dn, na.rm = TRUE))
    i=i+1
    hold_values[i] <- mean(dfx$tx_pwr_up / dfx$rx_pwr_up, na.rm = TRUE)
    i=i+1
    hold_values[i] <- mean(dfx$rx_pwr_up / dfx$snr_dn, na.rm = TRUE)
  }
  node_x_mac_add[z, ] <- hold_values #Add values as row
}
node_x_mac_add <- na.omit(node_x_mac_add) #Remove NA rows
node_x_mac_add[, -1] <- sapply(node_x_mac_add[, -1], as.numeric) #Convert col types to num
node_x_mac_add[sapply(node_x_mac_add, simplify = 'matrix', is.infinite)] <- NA #Replace inf with NA
NA2mean <- function(x) replace(x, is.na(x), mean(x, na.rm = TRUE))
node_x_mac_add <- replace(node_x_mac_add, TRUE, lapply(node_x_mac_add, NA2mean)) #Replace NA with col mean

# B. Applying PCA
prin_comp <- prcomp(node_x_mac_add[,-1], scale. = T, center = T )
std_dev <- prin_comp$sdev
pr_var <- std_dev^2
prop_varex <- pr_var/sum(pr_var)

summary(prin_comp)

# Variance % Top 10 PCA loadings
prop_varex[1:10]

# Variance of each Principal component
plot(prop_varex, xlab = "Principal Component",
     ylab = "Proportion of Variance Explained",
     type = "b")
plot(cumsum(prop_varex), xlab = "Principal Component",
     ylab = "Cumulative Proportion of Variance Explained",
     type = "b")
pca_x <- as.data.frame(prin_comp$x)
#plot(pca_x[,1:4], pch=16, col=rgb(0,0,0,0.5))

# C. PCA - K Means Clustering
library(cluster) # Needed for silhouette function

# Setup for k-means loop 
kmeansDat <- node_x_mac_add[,-1]
km.out <- list()
sil.out <- list()
x <- vector()
y <- vector()
minClust <- 2      # Hypothesized minimum number of segments
maxClust <- 6     # Hypothesized maximum number of segments

# Compute k-means clustering over various clusters, k, from minClust to maxClust
for (centr in minClust:maxClust) {
  i <- centr-(minClust-1) # relevels start as 1, and increases with centr
  set.seed(11) # For reproducibility
  km.out[i] <- list(kmeans(kmeansDat, centers = centr, nstart = 25, iter.max=1000))
  sil.out[i] <- list(silhouette(km.out[[i]][[1]], dist(kmeansDat))) # Running the k-means to find optinum cluster size
  # Used for plotting silhouette average widths
  x[i] = centr  # value of k
  y[i] = summary(sil.out[[i]])[[4]]  # Silhouette average width
}


# plot the silhouette average widths for the choice of clusters. 
# The best cluster is the one with the largest silhouette average width
# Plot silhouette results to find best number of clusters; closer to 1 is better
ggplot(data = data.frame(x, y), aes(x, y)) + 
  geom_point(size=3) + 
  geom_line() +
  xlab("Number of Cluster Centers") +
  ylab("Silhouette Average Width") +
  ggtitle("Silhouette Average Width as Cluster Center Varies")


# Applying k-means
set.seed(12)
k2 <- kmeans(pca_x, 2, nstart=25, iter.max=1000)

library(RColorBrewer)
palette(alpha(brewer.pal(9,'Set1'), 0.8))
plot(pca_x[,1:5], col=k2$clust, pch=16)

# Interactive 2D plot
k2_pca_df <- cbind(pca_x, group=k2$clust, mac=node_x_mac_add$src_node_id)
gg2 <- ggplot(k2_pca_df) +
  geom_point(aes(x=PC1, y=PC3, col=factor(group), text=mac), size=2) +
  labs(title = "Visualizing K-Means Clusters Against First Two Principal Components") +
  scale_color_brewer(name="", palette = "Set1")
# plotly for inteactivity
plotly1 <- ggplotly(gg2, tooltip = c("text", "x", "y")) %>%
  layout(legend = list(x=.9, y=.99))

# Boxplot
boxplot(node_x_mac_add$NA_Percentage ~ k2$cluster,
        xlab='Cluster', ylab='NA %',
        main='NA % by cluster')

boxplot(node_x_mac_add$mean_snr_dn ~ k2$cluster,
        xlab='Cluster', ylab='Mean snr_dn value',
        main='Mean snr_dn by Cluster')

boxplot(node_x_mac_add$mean_pathloss ~ k2$cluster,
        xlab='Cluster', ylab='Mean pathloss',
        main='Mean pathloss by Cluster')


