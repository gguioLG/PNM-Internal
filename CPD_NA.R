# Script Adds a column in the mac dataset to find out the occurrence (count) of 3/5/10 consecutive NAs for each mac in mac_snrdn/pathloss
# Also Adds a column of most consecutive of NA for each mac

# Dummy dataset to test functions of NAs
dataset <- data.frame(input = c("a","b","b","a","a","c","a","a","a","a","b","c","s", "r"))
dataset$counter <- c("1",NA,NA,"9",NA,NA, NA,"1","1","2",NA,NA,"1","4")
dataset[is.na(dataset)] = 9999

# Create NA functions
# Most consecutive NA in a mac series
consecutive_NA = function(x, val = 9999) {
  with(rle(x), max(lengths[values == val]))
}
consecutive_NA(dataset$counter)

# Number of X consecutive NA in a mac series
X_consecutive_NA = function(x, val = 9999) {
  with(rle(x), lengths[values == val])
}
sum(X_consecutive_NA(dataset$counter) == 2) # X as 2

# CPD  
#Create dummy list to hold aggregated mac addresses feature list
mac_add_DS <- data.frame(src_node_id=character(), pathloss_max_consecutive_NA=character(),snrdn_max_consecutive_NA=character(), snrdn_3_consecutive_NA=character(),
                         pathloss_3_consecutive_NA=character(), snrdn_5_consecutive_NA=character(), pathloss_5_consecutive_NA=character(),
                         snrdn_10_consecutive_NA=character(),pathloss_10_consecutive_NA=character(),
                         stringsAsFactors=FALSE)

nodes_names_list <- unique(NodeData$node_name) #list unique nodes
mac_DF <- list() #Reference list that will hold node + list mac add DF
for( i in nodes_names_list){ #change to nodes_name_list
  df_temp <- CPEData_n[CPEData_n$node_name == i,] #Subset based on node_name
  mac_DF[[i]]  <- split(df_temp ,df_temp$src_node_id) #Split by mac add, create DF
}

j = 1
counter = 1
for (j in 1: length(nodes_names_list)){
  z=1
  for (z in 1: length(mac_DF[[j]])){ 
    dfx <- mac_DF[[j]][[z]] 
    hold_values <- list() #create an empty list to hold values
    i=1
    while (i < 8) {  
      hold_values[i] <- unique(dfx$src_node_id) #Mac add
      i=i+1
      hold_values[i] <- consecutive_NA(dfx$pathloss)
      i=i+1
      hold_values[i] <- consecutive_NA(dfx$snr_dn)
      i=i+1
      hold_values[i] <- sum(X_consecutive_NA(dfx$snr_dn) == 3)
      i=i+1
      hold_values[i] <- sum(X_consecutive_NA(dfx$pathloss) == 3)
      i=i+1
      hold_values[i] <- sum(X_consecutive_NA(dfx$snr_dn) == 5)
      i=i+1
      hold_values[i] <- sum(X_consecutive_NA(dfx$pathloss) == 5)
      i=i+1
      hold_values[i] <- sum(X_consecutive_NA(dfx$snr_dn) == 10)
      i=i+1
      hold_values[i] <- sum(X_consecutive_NA(dfx$pathloss) == 10)
    }
    mac_add_DS[counter, ] <- hold_values #Add values as row
    counter = counter + 1
  }
}