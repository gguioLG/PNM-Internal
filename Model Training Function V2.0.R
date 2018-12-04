#################################################
## STEPS FOLLOWED
#################################################

# 1.  SETTING UP R 
# 2.  CREATE TRAIN TEST SPLIT
# 3.  CARET PARAMETERS FOR INDIVIDUAL MODELS
# 4.  CLASSIFICATION ALGORITHMS
# 5.  SERIALIZE THE BEST MODEL
# 6.  SAVE R SESSION

#################################################
## SETTING UP R
#################################################

# Add package library
# .libPaths( c( .libPaths(), "/data/mlserver/9.3.0/libraries/RServer-additional") )

#Package Calls

library(caret)
library(xgboost)
library(caretEnsemble)

#############################################################
## LOAD THE TRAINING DATA
#############################################################

model_training_data <- ExtractFeaturesFunction(referenceTest, CPEData)

###################################################
## LABELLING THE MAC ADDRESSES IN THE TRAINING DATA
###################################################

# Positive mac adresses from reference data set

mac_positive <- as.data.frame(unlist(strsplit(referenceTest$mac_addresses, ",")))
colnames(mac_positive) <- "src_node_id"

# mac_positive$src_node_id <- str_replace_all(mac_positive$src_node_id, ':', '')
# mac_positive$src_node_id <- str_replace_all(mac_positive$src_node_id, ',', '')

mac_positive <- mac_positive[! mac_positive$src_node_id == "NA", , drop=F]
mac_positive <- mac_positive %>% distinct(src_node_id, .keep_all = TRUE)
mac_positive$mac_label <- '1'


# Adding mac label - 0/1 variable for CPD cases.
# 0 is NEGATIVE CPD CASE
# 1 is POSITIVE CPD CASE 

model_training_data $mac_label <- mac_positive$mac_label[match(model_training_data$src_node_id, mac_positive$src_node_id)]

model_training_data$mac_label[is.na(model_training_data$mac_label)] <- '0'

model_training_data$mac_label <- as.factor(model_training_data$mac_label)

model_training_data  <- as.data.frame(model_training_data )


# #################################################
# ## MODEL TRAINING FUNCTION
# #################################################

ModTrnFunction <- function(df) {

set.seed(7)
  
df$mac_label <- as.numeric(levels(df$mac_label))[df$mac_label]

# Partition of the data set - Split data in train, test and validation data
# Validation Data = 10%
# Test Data = 30% of Remaining 90%
# Train Data = 70% of Remaining 90%

set.seed(7)

index <- partition(df, p = 0.1, id_col = "node_name")

validation_data <- index[[1]]
remaining_data <- index[[2]]

index_n <- partition(remaining_data, p = 0.3, id_col = "node_name")
test_data <- index_n[[1]]
train_data <- index_n[[2]]

train_label <- train_data[, "mac_label"]
test_label <- test_data[, "mac_label"]
validation_label <- validation_data[, "mac_label"]

train_matrix <- xgb.DMatrix(data = as.matrix(train_data[,!names(train_data) %in% c("node_name", "src_node_id", "mac_label")]), label = train_label )

test_matrix <- xgb.DMatrix(data = as.matrix(test_data[,!names(test_data) %in% c("node_name", "src_node_id","mac_label")]), label = test_label )

validation_matrix <- xgb.DMatrix(data = as.matrix(validation_data[,!names(test_data) %in% c("node_name", "src_node_id","mac_label")]), label = validation_label )


##### XGBOOST CLASSIFICATION ALGORITHM #####
  
set.seed(7)

# Hyperparameter Tuning for XGBOOST

xgb_params <- list(objective = "binary:logistic",
                   eta = 0.01,
                   max.depth = 3,
                   scale_pos_weight = 8,
                   gamma =1,
                   subsample=0.5,
                   colsample_bytree=0.8,
                   min_child_weight=1)

# Checking the test set AUC after training

bst_model <- xgb.train(params = xgb_params,  data = train_matrix,
                       watchlist = list(train = train_matrix, test = test_matrix),
                       nrounds = 100,
                       label = train_label,
                       verbose = TRUE,                                         
                       print.every_n = 1,
                       early_stopping_rounds =10,
                       eval.metric = "auc" )

# Selecting the best XGBOOST model

model_xgb <- xgboost(data = train_matrix, # the data           
                     max.depth = 3, # the maximum depth of each decision tree
                     nround =11, # max number of boosting iterations
                     objective = "binary:logistic", # the objective function 
                     eta = 0.01,
                     scale_pos_weight = 8,
                     gamma =1,
                     subsample=0.5,
                     colsample_bytree=0.8,
                     min_child_weight=1)

# CHECK the AUC on the validation data set

pred.xgb <- predict(model_xgb, validation_matrix)

# get & print the classification error

err <- mean(as.numeric(pred.xgb > 0.55) != validation_label)
print(paste("test-error=", err))
  
return(model_xgb)

}

# #################################################
# ## TRAINED MODEL
# #################################################

model_xgb <- ModTrnFunction(model_training_data)

# #################################################
# ## SERIALIZE THE BEST MODEL
# #################################################

xgb.save(model_xgb, "/data/mlserver/9.3.0/libraries/RServer-additional/best_model")


