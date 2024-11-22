library(zoo);
library(dplyr);
library(ggplot2);
library(data.table);
library(lubridate);
library(stringr);
library(caret)

library(h2o);
library(plumber);


# pr_run(pr("h2o_plumber.R"), port = 8000, docs = F)


# URL encode and decode
ENC = function(str) {
  return(URLencode(str, reserved = T))
}

DEC = function(str) {
  return(URLdecode(str))
}






# start h2o instance
h2o.init(name = "qa.analytics.h2o", ip = "localhost", port = 2022, nthreads = 4, max_mem_size = "4G")
# h2o.shutdown(prompt = FALSE)

# load model (unzip and then load)
model_path = "models/h2o.processed.gbm.productize.big/"
zipFile = "models_zip/gbm.productize.big.zip"
zip::unzip(zipFile, overwrite = T)
H2O_MODEL = h2o.loadModel(path = paste0(model_path, "qa_analytics_big.model"))

# load train levels
TRAIN_LEVELS = readRDS("tmp/tmp_levels_big.RData")

# load comp_no and costCenter mapping
comp_no_map    = fread("map/comp_no_map.csv")
costCenter_map = fread("map/costCenter_map.csv")
setnames(comp_no_map, "comp_no", "comp_no_data")
setnames(costCenter_map, "CostCenter", "CostCenter_data")



# failure string
FAIL_STRING = "Unable to Predict!"
NOT_TRAINED_STRING = "Not Trained on!"

# It is more costly to classify errors as correct data rather than classifying correct data as errors. Thus we can keep a higher cutoff for classifying correct data than for classifying errors
CUT_OFF_PROB_CORRECT_DATA = 0.99
CUT_OFF_PROB_ERROR = 0.9

# if the confidence in prediction is less than 0.95, then return failure
CUT_OFF_PROB = 0.9










#* @param comp_no
#* @param invoice_sum
#* @param invoice_currency
#* @param UserID
#* @param stamp_date
#* @param DocumentType
#* @param CostCenter
#* @param Country
#* @param Market
#* @param User
#* @get /qa-analytics-predict
#* @serializer cat
qa_analytics_predict = function(comp_no,
                                invoice_sum,
                                invoice_currency,
                                UserID,
                                stamp_date,
                                DocumentType,
                                CostCenter,
                                Country,
                                Market,
                                User) {
  
  # remove trailing whitespaces
  comp_no          = trimws(comp_no)
  invoice_sum      = trimws(invoice_sum) %>% as.numeric
  invoice_currency = trimws(invoice_currency)
  UserID           = trimws(UserID)
  stamp_date       = trimws(stamp_date)
  DocumentType     = trimws(CostCenter)
  CostCenter       = trimws(CostCenter)
  Country          = trimws(Country)
  Market           = trimws(Country)
  User             = trimws(Country)
  
  # compute client from comp_no_map
  client = comp_no_map[comp_no_data == comp_no, client]
  if(length(client) == 0) {
    return(FAIL_STRING)
  }
  
  # compute CostCenter.Function, CostCenter.Location and CostCenter.Overall from costCenter_map
  CostCenter.Function = costCenter_map[CostCenter_data == CostCenter, CostCenter.Function]
  CostCenter.Location = costCenter_map[CostCenter_data == CostCenter, CostCenter.Location]
  CostCenter.Overall  = costCenter_map[CostCenter_data == CostCenter, CostCenter.Overall]
  
  if(length(CostCenter.Function) == 0) {
    return(FAIL_STRING)
  }
  
  if(length(CostCenter.Location) == 0) {
    return(FAIL_STRING)
  }
  
  # compute stamp.DayofWeek, stamp.MonthofYear, stamp.hourofDay from stamp_date
  stamp_date         = as.POSIXct(stamp_date, format = "%d-%m-%Y %H:%M", tz = "UTC")
  stamp.DayofWeek    = weekdays(stamp_date)
  stamp.hourofDay    = lubridate::hour(stamp_date)
  stamp.minofHour    = (lubridate::minute(stamp_date) / 10) %>% as.integer
  stamp.minofHour    = paste0(10*stamp.minofHour, "-", 10*(stamp.minofHour+1))
  
  
  # disabling the below because new data (from April 6 2022 onwards) contains new set of UserID
  if(FALSE) {
    # check if data is trained on or not
    for(c in names(TRAIN_LEVELS)) {
      print(c)
      expr = paste0("!(", c, " %in% c(", "\"", paste0(TRAIN_LEVELS[[c]], collapse = "\", \""), "\"))")
      if(eval(parse(text = expr))) {
        return(NOT_TRAINED_STRING)
      }
    }
  }
  
  # create a h2o frame
  dt = data.frame(client = client,
                  invoice_sum = invoice_sum,
                  invoice_currency = invoice_currency,
                  UserID = UserID,
                  stamp.DayofWeek = stamp.DayofWeek,
                  stamp.hourofDay = stamp.hourofDay,
                  stamp.minofHour = stamp.minofHour,
                  DocumentType = DocumentType,
                  CostCenter.Function = CostCenter.Function,
                  CostCenter.Location = CostCenter.Location,
                  CostCenter.Overall = CostCenter.Overall,
                  Country = Country,
                  Market = Market,
                  User = User) %>%
    as.h2o
  
  # predict
  prediction = h2o.predict(object = H2O_MODEL, newdata = dt) %>%
    as.data.table
  prediction[, predict.new := predict]
  prediction[Correct.Data < CUT_OFF_PROB_CORRECT_DATA & Error < CUT_OFF_PROB_ERROR,
             predict.new := FAIL_STRING]
  
  return(prediction$predict.new %>% as.character)
  
}


# browser
# http://127.0.0.1:8000/audit-mate?comp_no=P8210&invoice_sum=93357&invoice_currency=XPF&UserID=Shirke%2C%20Sumita&stamp_date=01-01-2021%2004%3A47&DocumentType=DEF&CostCenter=PPPTKK&Country=FRENCH%20POLYNESIA%20%20&Market=INTL&User=WNS%20User
# excel
# =CONCATENATE("http://127.0.0.1:8000/qa-analytics-predict?", "comp_no=", ENCODEURL(A2), "&", "invoice_sum=", ENCODEURL(E2), "&", "invoice_currency=", ENCODEURL(F2), "&", "UserID=", ENCODEURL(L2), "&", "stamp_date=", ENCODEURL(TEXT(M2, "dd-mm-yyyy hh:mm")), "&", "DocumentType=", ENCODEURL(P2), "&", "CostCenter=", ENCODEURL(R2), "&", "Country=", ENCODEURL(V2), "&", "Market=", ENCODEURL(W2), "&", "User=", ENCODEURL(X2))






# input_file = "plumber_test/processed_from_06_April_2022_onwards.csv"
# output_file = "plumber_test/processed_from_06_April_2022_onwards_out.csv"
#* @param input_file
#* @param output_file
#* @get /qa-analytics-predict-batch
#* @serializer cat
qa_analytics_predict_batch = function(input_file, output_file) {
  
  if(!file.exists(input_file)) {
    return("Input File doesn't exist!")
  }
  
  if(file.exists(output_file)) {
    return("Output File alreadt exist!")
  }
  
  dt = fread(input_file)
  org_cols = names(dt)
  
  dt[, Error_Status_Numeric := ifelse(`Error Status` == "Error", 1, 0)]
  
  # remove trailing whitespaces
  dt[, comp_no          := trimws(comp_no) %>% toupper]
  dt[, invoice_sum      := trimws(invoice_sum) %>% as.numeric]
  dt[, invoice_currency := trimws(invoice_currency) %>% toupper]
  dt[, UserID           := trimws(UserID)]
  dt[, stamp_date       := trimws(stamp_date)]
  dt[, DocumentType     := trimws(DocumentType) %>% toupper]
  dt[, CostCenter       := trimws(CostCenter) %>% toupper]
  dt[, Country          := trimws(Country) %>% toupper]
  dt[, Market           := trimws(Market)]
  dt[, User             := trimws(User)]
  
  # compute client from comp_no_map
  dt = merge(dt, comp_no_map[, .(comp_no = comp_no_data, client)],
             by = "comp_no", all.x = T)
  
  # compute CostCenter.Function, CostCenter.Location and CostCenter.Overall from costCenter_map
  dt = merge(dt, costCenter_map[, .(CostCenter = CostCenter_data,
                                    CostCenter.Function, CostCenter.Location, CostCenter.Overall)],
             by = "CostCenter", all.x = T)
  
  # compute stamp.DayofWeek, stamp.MonthofYear, stamp.hourofDay from stamp_date
  dt[, stamp_date2        := as.POSIXct(stamp_date, format = "%d-%m-%Y %H:%M", tz = "UTC")]
  dt[, stamp.DayofWeek    := weekdays(stamp_date2)]
  dt[, stamp.hourofDay    := lubridate::hour(stamp_date2)]
  dt[, stamp.minofHour    := (lubridate::minute(stamp_date2) / 10) %>% as.integer]
  dt[, stamp.minofHour    := paste0(10*stamp.minofHour, "-", 10*(stamp.minofHour+1))]
  
  # find row indices which are used in training
  dt[, used_in_train__ := TRUE]
  # disabling the below because new data (from April 6 2022 onwards) contains new set of UserID, Country, client, CostCenter.*
  if(FALSE) {
    for(c in names(TRAIN_LEVELS)) {
      print(c)
      expr = paste0("!(", c, " %in% c(", "\"", paste0(TRAIN_LEVELS[[c]], collapse = "\", \""), "\"))")
      dt[eval(parse(text = expr)), used_in_train__ := FALSE]
    }
  }
  
  # create a h2o frame
  h2o_df = as.h2o(dt)
  
  # predict
  prediction = h2o.predict(object = H2O_MODEL, newdata = h2o_df) %>%
    as.data.table
  dt = cbind(dt, prediction)
  dt[, predict.new := predict]
  dt[Correct.Data < CUT_OFF_PROB_CORRECT_DATA & Error < CUT_OFF_PROB_ERROR,
     predict.new := FAIL_STRING]
  dt[used_in_train__ == FALSE, predict.new := NOT_TRAINED_STRING]

  # clean
  remove(h2o_df)
  dt = dt[, c(org_cols, "predict.new"), with = F]
  setnames(dt, "predict.new", "predicted.Error.Status")
  setnames(dt, "Error Status", "Error.Status")
  
  # dt[, .N, .(Error.Status, predicted.Error.Status)][order(Error.Status, predicted.Error.Status)]
  
  # write to disk
  fwrite(dt, output_file)
  
  return("Output File generated!")
  
}

# browser: http://127.0.0.1:8000/qa-analytics-predict-batch?input_file=plumber_test%2Fplumber_test_unseen.csv&output_file=plumber_test%2Fplumber_test_unseen_out.csv
# excel
# =CONCATENATE("http://127.0.0.1:8000/qa-analytics-predict-batch?input_file=", ENCODEURL("plumber_test/plumber_test_unseen.csv"), "&output_file=", ENCODEURL("plumber_test/plumber_test_unseen_out.csv"))







