import os
import zipfile
import h2o
from h2o.grid.grid_search import H2OGridSearch
from h2o.frame import H2OFrame
from h2o.estimators.gbm import H2OGradientBoostingEstimator
import pandas as pd
import numpy as np

# set the correct path
os.chdir('C:/Users/nikhi/Dropbox/GitHub/audit-mate')

# Initialize H2O cluster
h2o.init(name="nikhil.h2o", ip="localhost", port=2022, nthreads=8, max_mem_size="24G")
h2o.cluster_info()
# h2o.shutdown(prompt = False)

# Import train/test datasets
processed_train = h2o.import_file(path="masked/train.csv", destination_frame="processed.train")
processed_train_big = h2o.import_file(path="masked/train_big.csv", destination_frame="processed.train.big")
processed_test = h2o.import_file(path="masked/test.csv", destination_frame="processed.test")

# Define Y and X variables
Y = "Error_Status"
X = ["invoice_currency", "UserID", "DocumentType", "Country", "Market", "User",
     "stamp.DayofWeek", "stamp.hourofDay", "stamp.minofHour", "client",
     "CostCenter.Overall", "CostCenter.Function", "CostCenter.Location", "invoice_sum"]

# Fit GBM model
processed_gbm = H2OGradientBoostingEstimator(balance_classes=True,
                                             ntrees=75, max_depth=40,
                                             learn_rate=0.1, sample_rate=0.5,
                                             min_rows=5, nfolds=10, seed=54321)
processed_gbm.train(x=X, y=Y, training_frame=processed_train)

# Save model
model_path = "models/h2o.processed.gbm.productize/"
if not os.path.exists(model_path):
    os.makedirs(model_path)

h2o.save_model(model=processed_gbm, path=model_path, force=True, filename="gbm.model")

# Zip the model files
zip_file = "models_zip/gbm.productize.zip"
if os.path.exists(zip_file):
    os.remove(zip_file)

with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(model_path):
        for file in files:
            zipf.write(os.path.join(root, file))

# Fit GBM model on big train data
processed_gbm_big = H2OGradientBoostingEstimator(balance_classes=True,
                                                 ntrees=75, max_depth=40,
                                                 learn_rate=0.1, sample_rate=0.5,
                                                 min_rows=5, nfolds=10, seed=54321)
processed_gbm_big.train(x=X, y=Y, training_frame=processed_train_big)

# Save big model
model_path_big = "models/h2o.processed.gbm.productize.big/"
if not os.path.exists(model_path_big):
    os.makedirs(model_path_big)

h2o.save_model(model=processed_gbm_big, path=model_path_big, force=True, filename="gbm.big..model")

# Zip the big model files
zip_file_big = "models_zip/gbm.productize.big.zip"
if os.path.exists(zip_file_big):
    os.remove(zip_file_big)

with zipfile.ZipFile(zip_file_big, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(model_path_big):
        for file in files:
            zipf.write(os.path.join(root, file))

# Evaluate models
print(processed_gbm.confusion_matrix())
print(processed_gbm.varimp())
print(processed_gbm.model_performance(test_data=processed_test))

print(processed_gbm_big.confusion_matrix())
print(processed_gbm_big.varimp())
print(processed_gbm_big.model_performance(test_data=processed_test))

# Hyperparameter tuning
gbm_hyperparams = {
    "ntrees": list(range(50, 101, 10)),
    "max_depth": list(range(20, 51, 5)),
    "min_rows": list(range(5, 11, 1)),
    "sample_rate": [x / 10 for x in range(1, 11)],
    "learn_rate": [x / 10 for x in range(1, 6)]
}

search_criteria = {"strategy": "RandomDiscrete", "max_models": 25, "seed": 1}

grid = H2OGridSearch(H2OGradientBoostingEstimator(balance_classes=True, nfolds=10, seed=54321),
                     hyper_params=gbm_hyperparams, search_criteria=search_criteria)

grid.train(x=X, y=Y, training_frame=processed_train, validation_frame=processed_test)

# Save grid results
grid_dir = "models/h2o.processed.gbm.grid.search/"
if not os.path.exists(grid_dir):
    os.makedirs(grid_dir)

h2o.save_grid(grid_directory=grid_dir, grid_id=grid.grid_id)

# Analyze grid results
summary_table = grid.get_grid().as_data_frame()
summary_table["err_misclassifications"] = [
    h2o.get_model(model_id).confusion_matrix(processed_test)["Rate"][2] for model_id in grid.model_ids
]
correlation = summary_table.corr()[["logloss", "err_misclassifications"]]
print(correlation)

# Predictions
predictions = processed_gbm.predict(processed_test)
predictions = predictions.as_data_frame()
predictions["predict.new"] = predictions["predict"]
predictions.loc[(predictions["Correct Data"] < 0.95) & (predictions["Error"] < 0.95), "predict.new"] = "Unable to predict!"

processed_test["h2o.predict"] = h2o.H2OFrame(predictions["predict"])
processed_test["h2o.predict.new"] = h2o.H2OFrame(predictions["predict.new"])
