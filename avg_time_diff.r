df_step3 <- mac_feature_list %>%
  inner_join(unique(select(CPEData_n, node_name, src_node_id)), by=("src_node_id" = "src_node_id"))


###################### STEP 10
##### Add a column in the data frame mac_feature_list to find out the average
##### time difference between successive (3 consecutive NAs) for each mac in mac_snrdn

#test <- mac_snrdn[2351326:2351413,]

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
  return(mean(diff(hourstamps)))
}


test2 <- data.frame(src_node_id = character(), hour_stamp=character(), snr_dn = double(), pathloss = double())
test2 <- rbind(test2, src_node_id=as.data.frame(rep((letters[seq(from = 1, to = 1)]),15)), make.row.names=FALSE)

test2 <- cbind(test2, as.data.frame(c(seq(1,15))))
test2 <- cbind(test2, as.data.frame(c(3,2,1,NA,NA,NA,3,4,5,NA,NA,NA,4,4,6)))
test2 <- cbind(test2, as.data.frame(c(3,NA,NA,NA,NA,NA,6,7,NA,NA,NA,NA,NA,9,NA)))
colnames(test2) <- c("src_node_id", "hour_stamp","snr_dn", "pathloss")


aux <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_3NA_snrdn = avg_time_diff(., 3)) %>%
  distinct(avg_time_3NA_snrdn)
aux
aux2 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_5NA_snrdn = avg_time_diff(., 5)) %>%
  distinct(avg_time_5NA_snrdn)
aux2
aux3 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_10NA_snrdn = avg_time_diff(., 10)) %>%
  distinct(avg_time_10NA_snrdn)
aux3

aux4 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_3NA_pathloss = avg_time_diff(., 3, FALSE)) %>%
  distinct(avg_time_3NA_pathloss)
aux4
aux5 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_5NA_pathloss = avg_time_diff(., 5, FALSE)) %>%
  distinct(avg_time_5NA_pathloss)
aux5
aux6 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_10NA_pathloss = avg_time_diff(., 10, FALSE)) %>%
  distinct(avg_time_10NA_pathloss)
aux6


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
  return(mean(diff(hourstamps)))
}

aux7 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_between_3NA_snrdn = avg_time_diff_between_na(., 3)) %>%
  distinct(avg_time_between_3NA_snrdn)
aux7
aux8 <- test2 %>% group_by(src_node_id) %>% mutate(avg_time_between_3NA_pathloss = avg_time_diff_between_na(., 5, FALSE)) %>%
  distinct(avg_time_between_3NA_pathloss)
aux8




