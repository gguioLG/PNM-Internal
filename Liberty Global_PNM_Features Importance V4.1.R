#################################################
## SETTING UP R
#################################################

# Add package library
# .libPaths( c( .libPaths(), "/data/mlserver/9.3.0/libraries/RServer-additional") )

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
library(ggplot2)
library(corrplot)
library(doParallel)
library(glmnet)

# #################################################
# ## PARELLEL PROCESSING
# #################################################
 
# Register Parellel processing

# > sessionInfo()
# R version 3.4.3 (2017-11-30)
# Platform: x86_64-pc-linux-gnu (64-bit)
# Running under: Ubuntu 16.04.4 LTS

# Checking the number of cores

parallel::detectCores(logical = FALSE) # Physical cores

# [1] 16

parallel::detectCores(logical = TRUE)  # Logical cores

# [1] 32

# cores<-detectCores()
# cl <- makeCluster(cores[1]-10)
# registerDoParallel(cl)

# Choosing parellel processing to speed up execution
# Dont choose all the cores because R needs some memory free for preocessing

registerDoParallel(cores = 16) # Specify Physical cores, it works most of the time. 


# #################################################
# ## Random forest for feature selection
# #################################################

control <- trainControl(method="repeatedcv", number=10, repeats=3)
metric <- "Kappa"
preProcess <- c("scale", "center")


model_rf <- train(mac_label ~ ., data = mac_new[,3:50], method = "rf", 
               preProcess = preProcess , metric=metric, trControl = control,
               allowParallel = TRUE)

importance_rf <- varImp(model_rf)

# summarize importance

print(importance_rf)

# plot importance

plot(importance_rf)


# ###########################################################
# ## BAGGED CART for feature selection
# ###########################################################

control <- trainControl(method="repeatedcv", number=10, repeats=3)
metric <- "Kappa"
preProcess <- c("scale", "center")

model_tb <- train(mac_label ~ ., data = mac_new[,3:50], method = "treebag", 
                  preProcess = preProcess , metric=metric, trControl = control,
                  allowParallel = TRUE)

importance_tb <- varImp(model_tb)

# summarize importance

print(importance_tb)

# plot importance

plot(importance_tb)

# ############################################################
# ## Recursive Feature Elimination (RFE) for feature selection
# ############################################################

control <- rfeControl(functions=rfFuncs, method="cv", number=10)

model_rfe <- rfe(mac_new[,3:49], mac_new[,50], sizes=c(1:47), 
                 rfeControl=control, allowParallel = TRUE)

# summarize the results

print(model_rfe)

# list the chosen features

predictors(model_rfe)

# plot the results

plot(model_rfe, type=c("g", "o"))

# Un-Register Parellel processing

registerDoSEQ()

# ###########################################################
# ## Correlated variables
# ###########################################################

correlationMatrix <- cor(mac_new[,3:49])

hc <- findCorrelation(correlationMatrix, cutoff=0.75)

hc = sort(hc)

correlationMatrix <- as.data.frame(correlationMatrix)

# Uncorrelated features (corr < 0.75)

colnames(correlationMatrix[,-c(hc)])

# Correlated features (corr < 0.75)

colnames(correlationMatrix[,c(hc)])

# ###########################################################
# ## Common Features between 4 methods ()
# ###########################################################

# Getting features above threshold for RF

df <- data.frame(importance_rf[1])
setDT(df, keep.rownames = TRUE)[]
rf_cols <- df$rn[df$Overall > 20]

# Getting features above threshold for TB

df <- data.frame(importance_tb[1])
setDT(df, keep.rownames = TRUE)[]
tb_cols <- df$rn[df$Overall > 20]

rfe_cols <- predictors(model_rfe)

uncor_cols <- colnames(correlationMatrix[,-c(hc)])

Reduce(intersect, list(rf_cols,tb_cols,rfe_cols,uncor_cols))

# [1] "snrdn_ccor"    "pathloss_ccor"

Reduce(intersect, list(tb_cols,rfe_cols,uncor_cols))

# [1] "pathloss_ccor"  "pathloss_delta" "pathloss_edr"   "pathloss_min"  
# [5] "pathloss_sd"    "snrdn_ccor"     "snrdn_delta"    "snrdn_dtw"     
# [9] "snrdn_lcss"     "snrdn_sd"

# ###########################################################
# ## LASSO REGRESSION FOR FEATURE SELECTION
# ###########################################################

x <- model.matrix(mac_label~., mac_new[,3:50])[,-1]

y <- as.factor(mac_new$mac_label)

glmmod <- glmnet(x, y, alpha=1, family="binomial")

# Plot variable coefficients vs. shrinkage parameter lambda.

plot(glmmod, xvar="lambda")

# cross validation to select lambda

x <- model.matrix(mac_label~., mac_new[,3:50])[,-1]

y <- as.numeric(mac_new$mac_label)

cv.glmmod <- cv.glmnet(x, y, alpha=1)

# coefficients with the minimum cross-validation error:

# CF <- as.matrix(coef(cv.glmmod, cv.glmmod$lambda.min))

# CF[CF!=0,]

# coefficients with the "largest value of lambda such that error 
# is within 1 standard error of the minimum

CF <- as.matrix(coef(cv.glmmod, cv.glmmod$lambda.1se))

CF[CF!=0,]

# (Intercept)      snrdn_sd  pathloss_min   pathloss_sd     snrdn_edr 
# 0.9650895362  0.0225167083 -0.0014804724  0.0292480849  0.0005082038 
# snrdn_lcss    snrdn_ccor pathloss_ccor 
# 0.0010409068 -0.0041293332 -0.0143595208 


# #################################################
# ## SAVE R SESSION
# #################################################

save.image(file='Feature_Importance.RData')

# To load this R session:

# load('Feature_Importance.RData')

