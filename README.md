1. `model.R` reads the test and train datasets and performs model training. I use Gradient Boosted Machine (GBM) with balanced classes as the algorithm. The trained model is stored for prediction later. There is also some code depicting confusion matrices, hyper-parameter serach etc. A python version `model.py` is also available.
2. `h2o_plumber.R` reads the saved model file and listens for API calls for predictions. These API calls can be made from any platform: excel, web, python etc.
3. Helpful scripts are present in `cost_centre_mapping.R`, `dpm_map.R` (hidden), `masking.R`, `pre_process_data.R`, `prepare_data.R`, `print_model_tree.R`, and `process_dates_and_timestamps.R`
4. Train and test data (after masking) is present in folder `masked`.
