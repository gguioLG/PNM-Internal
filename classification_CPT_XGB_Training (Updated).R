#CPD Classification Model

#0) Node Selection - which node should we be looking at? --HMM or CPT?

# Work out how to create functions for each of these - should make it easier later to add/remove etc.

#1) For a Node we need:
# SNR UP DATASET
# List of Mac Addresses
# Mac Address Polling Data

#2) Now we generate our similairty metrics for each Mac address with the Node

#3) #XGBoost - classifier - need dataset in the form of

#MAC ADDRESS /IDENTIFER
# NODE WITH CPD ISSUE
# SIMILARITY METRICS - these are the comparasion with the SNR UP DF
# PNM METRICS SUMMARY 
# LINEAR REGRESSION ON MAC ADDRESS
# CHANGE POINTS OF MAC ANALYSIS for Severity
# CPD FAULT TRUE/FALSE

##############################################
## COMMAND LINE STUFF
##############################################

#Check if running in RStudio
isRStudio <- ifelse(Sys.getenv("RSTUDIO") ==1, TRUE,FALSE)
if(isRStudio){
  isCmdLine <- FALSE 
  print("Running Interactively in RStudio")
}else{
 isCmdLine <- TRUE 
}

#Load Config file
source("C:/Users/bsutton/Documents/GitHub/GDA_CPD/R/user/bsutton/config.R")

#RUN NAMES FOR VERSIONING
trainingRunName <- 'V5_AllTestCases_TSDistances_NAFiller'
trainingRunDate <- ymd(Sys.Date())

#LOG-FILE
# writeToLogFile <- TRUE
# if(writeToLogFile){
#   logFile <- paste0('C:/Users/bsutton/Documents/GitHub/GDA_CPD/R/user/bsutton/logs/',runDate,'_',as.integer(Sys.time()),'_',runName,'.log')
#   file.create(logFile)
#   sink(file = logFile,append = TRUE)
# }

#Load Libraries
loadLibraries(libraryList)

#Training Flag
TRAIN = TRUE

#Generate Features Flag
#If false will load from disk
FEATURES_GENERATE = TRUE
cat("Generate features flag set to:",FEATURES_GENERATE)

#RUN CONFIG
WRITE_TO_DISK <- TRUE
cat("Write to disk flag set to:",WRITE_TO_DISK)

#Check directories exist
if(!dir.exists(outputDir)){
  cat('Output Directory does not exists.Creating')
  dir.create(outputDir)
}

#Check directories exist
if(!dir.exists(outputTrainingDir)){
  cat('Output Training Directory does not exists.Creating')
  dir.create(outputTrainingDir)
}

cat("Output directory:",outputDir)
cat("Output Training directory:",outputTrainingDir)

#####################################################################
## TRAINING OUTPUT DIRECTORY
#####################################################################
#Create output directory if needed
trainingRunOutputDirectory <- paste0(outputTrainingDir,'/',trainingRunDate,'_',trainingRunName)

trainingRunOutputDirectory <- createDirectoryIterator(trainingRunOutputDirectory)

cat(trainingRunOutputDirectory,fill=TRUE)

#####################################################################
## LOAD TRAINING REFERENCE DATA
#####################################################################
#Only run new features generate if flag is set - otherwise read from disk
if(FEATURES_GENERATE){
#1) Get training reference data
nodeListBrokenPiecesReference <- getTrainingReferenceData(brokenPieces = TRUE,test = TRUE, country='CH')

#Testing for One Case
#nodeListBrokenPiecesReference <- nodeListBrokenPiecesReference[1,]

cat("Total test cases:", nrow(nodeListBrokenPiecesReference))

#####################################################################
## PROCESS TRAINING REFERENCE DATA
#####################################################################
#2) Process each test case

#Initialise Overall dataset
macFeaturesTrainingSet <- initialiseMacFormatTibble(macFeatures = macFeatureVariables,identifierFeatures = c('CASE_REF','NODE_ID','VERTEX_TOPO_NODE_ID','MAC_ADDRESS'))

#For Each test set generate the features we require
for(i in 1:nrow(nodeListBrokenPiecesReference)){
  macFeaturesTrainingSet <- rbind(macFeaturesTrainingSet,processTestCase(nodeListBrokenPiecesReference[i,]))
}

  ############################################################
### Create Output Directories
############################################################
macFeaturesfileName <- macFeaturesfileName
#Save features to disk
if(WRITE_TO_DISK){
  #Check file exists
  macFeaturesFile <- paste0(trainingRunOutputDirectory,'/',macFeaturesfileName)
  if(file.exists(macFeaturesFile)){
    stop("Mac features file already exsits.")
  }else{
    write.csv(x = macFeaturesTrainingSet,file = macFeaturesFile,sep = ',',na = 'NA',row.names = FALSE,col.names = TRUE)
  }
}

}else if(!FEATURES_GENERATE){
  #######################################################
  ## Load Featureset from disk
  ########################################################
  #Check file exists
  macFeaturesFile <- paste0(trainingRunOutputDirectory,'/',macFeaturesfileName)
  if(file.exists(macFeaturesFile)){
    macFeaturesTrainingSet <- read.csv(file = macFeaturesFile,header = TRUE,sep = ',')
  }else{
    stop("Mac address features data does not exist.")
  }
  #trainingSet <- dplyr::select(trainingSet,-c("X"))
}

#################################################
## CHECK MAC ADDRESS TRAINING DATA
#################################################
#Dataframe exists
if(!exists("macFeaturesTrainingSet")){
  stop("Mac features dataframe does not exist.")
}

#Row count
trainingRowCount <- nrow(macFeaturesTrainingSet)
if(trainingRowCount == 0){
  stop("Zero rows in training set")
}else{
  cat("Number of training set rows:",trainingRowCount,fill=TRUE)
}

#Number of Mac addresses
trainingDataMacCount <- unique(macFeaturesTrainingSet$MAC_ADDRESS) %>% length()
if(trainingDataMacCount <= 1){
  warning("Only one Mac address for training")
}else{
  cat("Number of mac address for training:",trainingDataMacCount,fill=TRUE)
}
#Column names
trainingSetCols <- colnames(macFeaturesTrainingSet)
cat("Number of columns for training:",length(trainingSetCols),fill=TRUE)
cat("Column names:",trainingSetCols,fill=TRUE)

#NA's per column

#################################################
## MODEL TRAINING
#################################################
#Initial use of XGBoost for ease of use
set.seed(12345)

#Selecting the target variable
target <- "IS_CPD"

#Features that will not be used as part of the model training
featuresToRemove <- c("MAC_ADDRESS", "CASE_REF", "NODE_ID", "VERTEX_TOPO_NODE_ID")
extras <- c("erpd_pathloss","erpd_snr_dn","lrStdError_snr_dn","lrStdError_pathloss")
featuresToRemove <- c(featuresToRemove)
allFeaturesToRemove <- c(featuresToRemove,target)

#Training Featres
colnames(macFeaturesTrainingSet)

#Testing - Forcing Target to verify Prediciton functions
#trainingSet$CPD_TESTING_FLAG <- trainingSet$IS_CPD

#Calculate Train and Test Sets
testTrain <- calculateTestTrain(dataset = macFeaturesTrainingSet, trainPercent = 0.7, testPercent = 0.3, grouping = 'CASE_REF')
training <- testTrain[[1]]
testing <- testTrain[[2]]

#Need to add some sampling here - likely have to undersample the number of CPD == - cases
TRAIN_SAMPLING <- TRUE
if(TRAIN_SAMPLING){
  numberOfTrainingPositive <- sum(training$IS_CPD)
  numberOfTrainingFalse <- filter(training, IS_CPD ==0) %>% count() %>% as.double()
  currentTrueRate <- numberOfTrainingPositive/nrow(training)
  cat("Current True Rate:", currentTrueRate,fill = TRUE)
  
  #DOWNSAMPLE THE MAJORITY CLASS
  targetTrueRate <- 0.01
    trueRate <- numberOfTrainingPositive/ (numberOfTrainingPositive + numberOfTrainingFalse)
    targetNumberOfMac <- (numberOfTrainingPositive/targetTrueRate) - numberOfTrainingPositive
  negativeCases <- filter(training, IS_CPD ==0) %>% dplyr::select(MAC_ADDRESS) 
  sampledMac <- sample(negativeCases[['MAC_ADDRESS']], targetNumberOfMac)
  training <- filter(training,(MAC_ADDRESS %in% sampledMac | IS_CPD == 1))
  nrow(training)
}

#Convert to Xgb datsets
trainMatrix <- generateXGMatrix(training, target = target, removeFeatures = allFeaturesToRemove)
testMatrix <- generateXGMatrix(testing, target = target, removeFeatures = allFeaturesToRemove)

#Train XGBoost
trainedXGB <- trainXGBModel(test = testMatrix, train = trainMatrix)

#Features in model
cat("Features in trained model:",trainedXGB$feature_names,fill=TRUE)

#Predictions on test set

#Calculate Performance

#Save to disk 


#For Model lets make predictions on the test set
testPredictions <- predict(trainedXGB, testMatrix, type = "response")

#Join Predictions back to main dataset
testFeaturesPredictions <- cbind(testing,testPredictions)
testFeaturesPredictions$Prediction <- ifelse(testFeaturesPredictions$testPredictions >= 0.65,1,0)
testFeaturesPredictions<- testFeaturesPredictions %>% mutate(CorrectPrediction = if_else(IS_CPD == Prediction,1,0))

#Calcuate metrics for Confusion Matrix
confusionMetrics <- calculateConfusionMatrixMetrics(
  data.frame(actual = testFeaturesPredictions$IS_CPD,
             prediction = testFeaturesPredictions$Prediction)
  )

#High Level Stats
cat("###Model Classifier Stats###")
cat("Accuracy:",confusionMetrics$accuracy,fill=TRUE)
cat("Precision:",confusionMetrics$precision,fill=TRUE)
cat("Recall:",confusionMetrics$recall,fill=TRUE)
cat("Error Rate:",confusionMetrics$errorRate,fill=TRUE)
cat("True Positive:",confusionMetrics$truePositive, fill =TRUE)
cat("False Positive:", confusionMetrics$falsePositive, fill = TRUE)
cat("Actual Positive:",confusionMetrics$actualTrue, fill =TRUE)
confusionMetrics
#Print Confusion matrix

#Fweature Importance
xgb.importance(model = trainedXGB)
#Print ROC Curve


#Calculate Scale Weight
#scaleCaseWeight <- (nrow(trainSet) - sum(trainSet$IS_CPD)) / sum(trainSet$IS_CPD)

xgb$feature_names

xgb.importance(model = xgbModel)
xgbModel$feature_names
xgb.model.dt.tree(model = xgbModel)
# Generation of the output of the model
# First, a folder is created according to the name given in the outputName variable














importance_matrix <- xgb.importance(model = xgbModel)
print(importance_matrix)
xgb.plot.importance(importance_matrix = importance_matrix)

#Run Predicitions on test set
xgb.pred

setwd(basePath)
outputDir <- paste0("./Outputs/", outputName)
if (!file.exists(outputDir)) {
  dir.create(file.path(basePath, outputDir))
}


#################################################
## TOPOLOGY SEARCH ??
#################################################

library(igraph)

#Build igraph object for node topology
nodeTopology.igraph <- buildIgraphObjectNode(NODE_ID,current = TRUE)

#Assign Mac Address Classification
nodeTopology.igraph <- set_vertex_attr(nodeTopology.igraph, "MAC ADDRESS", index = V(nodeTopology.igraph), as.character(macFeatureResults$pred))

#Graph Search Time....

#################################################
## HIER CLUSTERING ??
#################################################
