#################################################
## STEPS FOLLOWED
#################################################

# 1.  SETTING UP R
# 2.  LOAD THE NEW DATA
# 3.  LOAD THE BEST MODEL (UNSERIALIZE)
# 4.  MODEL SCORING
# 5.  SAVE R SESSION


#################################################
## SETTING UP R
#################################################

# Add package library
# .libPaths( c( .libPaths(), "/data/mlserver/9.3.0/libraries/RServer-additional") )

#Package Calls

library(plyr)
library(data.table)
library(dplyr)
library(caret)
library(xgboost)


#############################################################
## LOAD THE NEW DATA
#############################################################

validation_data <- ExtractFeaturesFunction(referenceTest, CPEData)

validation_data$mac_label <- '0'

validation_data$mac_label <- as.numeric(validation_data$mac_label)

validation_label <- validation_data[, "mac_label"]
validation_matrix <- xgb.DMatrix(data = as.matrix(validation_data[,!names(validation_data) %in% c("node_name", "src_node_id","mac_label")]), label = validation_label)

# #################################################
# ## LOAD THE BEST MODEL (UNSERIALIZE)
# #################################################

best_model <- xgb.load("/data/mlserver/9.3.0/libraries/RServer-additional/best_model")

# #################################################
# ## MODEL SCORING
# #################################################

# make a predictions on "new data" using the final model

final_predictions <- predict(best_model, validation_matrix) 

validation_data$mac_label <- final_predictions

validation_data$mac_label <- ifelse(validation_data$mac_label > 0.55, "POSITIVE", "NEGATIVE")


# #################################################
# ## SAVE R SESSION
# #################################################

save.image(file='Model_Scoring.RData')

# To load this R session:

# load('Model_Scoring.RData')

