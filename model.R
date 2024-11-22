# Required libs
library(data.table);
library(magrittr);
library(stringr);
library(ggplot2);

# set the correct path
setwd('C:/Users/nikhi/Dropbox/GitHub/audit-mate')


# Load H2O and start up an H2O cluster
library(h2o)
h2o.init(name = "nikhil.h2o", ip = "localhost", port = 2022, nthreads = 8, max_mem_size = "24G")

h2o.clusterInfo()
# h2o.shutdown(prompt = FALSE)

# import train/test dataset
# train and test contain rows for which a decent number of categories are represented. train.big contains all data irrespective of whether categories are modestly represented or not. Note that test data always comes from the group which has decent number of categories. See clean_data.R
processed.train     = h2o.importFile(path = "masked/train.csv",
                                     destination_frame = "processed.train")
processed.train.big = h2o.importFile(path = "masked/train_big.csv",
                                     destination_frame = "processed.train.big")
processed.test      = h2o.importFile(path = "masked/test.csv",
                                     destination_frame = "processed.test")



# define Y and X vars
# ReasonCode or Error_Status
Y = "Error_Status"
# Y = "ReasonCode"
# include VendorID?
X = c("invoice_currency", "UserID", "DocumentType", "Country", "Market", "User", "stamp.DayofWeek", "stamp.hourofDay", "stamp.minofHour", "client", "CostCenter.Overall", "CostCenter.Function", "CostCenter.Location", "invoice_sum")

# Available models:
# infogram,targetencoder,deeplearning,glm,glrm,kmeans,naivebayes,pca,svd,drf,gbm,isolationforest,extendedisolationforest,aggregator,word2vec,stackedensemble,coxph,generic,gam,anovaglm,psvm,rulefit,upliftdrf,modelselection
# h2o.gbm, h2o.randomForest, h2o.deeplearning


# model fit
processed.gbm = h2o.gbm(training_frame = processed.train,
                        x = X, y = "Error_Status",
                        balance_classes = T,
                        ntrees = 75, max_depth = 40,
                        learn_rate = 0.1, sample_rate = 0.5, min_rows = 5,
                        nfolds = 10, seed = 54321)
model_path = "models/h2o.processed.gbm.productize/"
# save model files. also delete earlier model files
list.files(model_path, full.names = T) %>% file.remove
h2o.saveModel(processed.gbm, path = model_path, force = T, filename = "gbm.model")
# zip the model as well
zipFile = "models_zip/gbm.productize.zip"
if(file.exists(zipFile)) file.remove(zipFile)
zip::zip(zipfile = zipFile,
         files = list.files(model_path, full.names = T),
         compression_level = 9)
# load: unzip and then load
# zip::unzip(zipFile, overwrite = T)
# processed.gbm = h2o.loadModel(path = paste0(model_path, "gbm.model"))




# model fit (GBM) (big train data)
processed.gbm.big = h2o.gbm(training_frame = processed.train.big,
                            x = X, y = "Error_Status",
                            balance_classes = T,
                            ntrees = 75, max_depth = 40,
                            learn_rate = 0.1, sample_rate = 0.5, min_rows = 5,
                            nfolds = 10, seed = 54321)
model_path = "models/h2o.processed.gbm.productize.big/"
# save model files. also delete earlier model files
list.files(model_path, full.names = T) %>% file.remove
h2o.saveModel(processed.gbm.big, path = model_path, force = T, filename = "gbm.big.model")
# zip the model as well
zipFile = "models_zip/gbm.productize.big.zip"
if(file.exists(zipFile)) file.remove(zipFile)
zip::zip(zipfile = zipFile,
         files = list.files(model_path, full.names = T),
         compression_level = 9)
# load: unzip and then load
# zip::unzip(zipFile, overwrite = T)
# processed.gbm.big = h2o.loadModel(path = paste0(model_path, "gbm.big.model"))




# confusion matrix
h2o.confusionMatrix(processed.gbm)
# variable importance
h2o.varimp(processed.gbm)
# performance
h2o.confusionMatrix(processed.gbm, newdata = processed.test)
h2o.performance(model = processed.gbm, newdata = processed.test)


# confusion matrix
h2o.confusionMatrix(processed.gbm.big)
# variable importance
h2o.varimp(processed.gbm.big)
# performance
h2o.confusionMatrix(processed.gbm.big, newdata = processed.test)
h2o.performance(model = processed.gbm.big, newdata = processed.test)










# hyper-parameter search over gbm model
gbm_hyperparams = list(ntrees = seq(50, 100, 10),
                       max_depth = seq(20,50,5),
                       min_rows = seq(5, 10, 1),
                       sample_rate = seq(0.1, 1, 0.1),
                       learn_rate = seq(0.1, 0.5, 0.1))

# ideally there should be a validation dataset. For now I am using processed.test
gbm_hyperparam_grid = h2o.grid(algorithm = "gbm",
                               x = X, y = "Error_Status",
                               grid_id = "gbm_hyperparam_grid",
                               training_frame = processed.train,
                               validation_frame = processed.test,
                               hyper_params = gbm_hyperparams,
                               search_criteria = list(strategy = "RandomDiscrete",
                                                      max_models = 25, seed = 1),
                               # arguments passed to h2o.gbm
                               balance_classes = T, nfolds = 10, seed = 54321)

# save to disk
grid_dir = "models/h2o.processed.gbm.grid.search/"
h2o.saveGrid(grid_directory = grid_dir, grid_id = "gbm_hyperparam_grid")
# zip all the models in the grid
grid_files = list.files(grid_dir)
# remove all existing files
list.files(path = "models_zip", pattern = "gbm\\.grid\\.search_\\d+\\.zip", full.names = T) %>% file.remove
for(i in 1:length(grid_files)) {
  print(i)
  f = grid_files[i]
  zipFile = paste0("models_zip/gbm.grid.search_", i, ".zip")
  zip::zip(zipfile = zipFile,
           files = paste0(grid_dir, f),
           compression_level = 9)
}
# load: unzip and then load
# zipFiles = list.files(path = "models_zip", pattern = "gbm\\.grid\\.search_\\d+\\.zip", full.names = T)
# for(z in zipFiles) {
#   print(z)
#   zip::unzip(z, overwrite = T)
# }
# gbm_hyperparam_grid = h2o.loadGrid(grid_path = paste0(grid_dir, "gbm_hyperparam_grid"))







err_misclassifications = rep(NA, length(gbm_hyperparam_grid@model_ids))
for(i in 1:length(err_misclassifications)) {
  print(i)
  m = gbm_hyperparam_grid@model_ids[[i]]
  err = h2o.confusionMatrix(h2o.getModel(m), newdata = processed.test)[["Rate"]][2]
  err = err %>% str_split("/", simplify = T) %>% {.[1]} %>% str_split("=", simplify = T) %>% {.[2]} %>% as.integer
  err_misclassifications[i] = err
}

summary_table = as.data.frame(gbm_hyperparam_grid@summary_table)
setDT(summary_table)
summary_table[, err_misclassifications := err_misclassifications]
# see which hyper-parameters reduce err_misclassifications and logloss
summary_table[, -"model_ids"] %>% cor %>% {.[, c("logloss", "err_misclassifications")]}












# prediction
prediction = h2o.predict(object = processed.gbm, newdata = processed.test)
# if none of the prediction is above 0.99 then mark it as unable to predict
CUT_OFF_PROB = 0.95
prediction = as.data.table(prediction)
prediction[, predict.new := predict]
prediction[Correct.Data < CUT_OFF_PROB & Error < CUT_OFF_PROB, predict.new := "Unable to predict!"]
names(prediction)[2] = "Correct Data"


prediction = as.h2o(prediction)
processed.test$h2o.predict     = prediction$predict
processed.test$h2o.predict.new = prediction$predict.new

dt = as.data.table(processed.test)
dt = dt[, .(Error_Status, h2o.predict, h2o.predict.new)]

dt[, .N, .(Error_Status, h2o.predict)][order(Error_Status, h2o.predict)]
dt[, .N, .(Error_Status, h2o.predict.new)][order(Error_Status, h2o.predict.new)]




