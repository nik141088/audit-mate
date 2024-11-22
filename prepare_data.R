# Required libs
library(zoo);
library(dplyr);
library(ggplot2);
library(data.table);
library(lubridate);
library(stringr);
library(caret);

setwd('C:/Users/nikhi/Dropbox/GitHub/audit-mate')
source('masking.R')

# read unmasked data and add mask
masked = fread('unmasked/data.csv') %>% apply_mask


# create train and test data
# making sure all factors are equally represented in the test data
# ReasonCode or Error_Status. This is the dependent variable!
fac_cols = c("Error_Status", "invoice_currency", "UserID", "DocumentType", "Country", "Market", "User", "stamp.DayofWeek", "stamp.hourofDay", "stamp.minofHour", "client", "CostCenter.Overall", "CostCenter.Function", "CostCenter.Location")

# ML model suggests that the below are not very important (percentage importance < 1%)
# not_imp = c("User", "Market", "CostCenter.Overall")
# fac_cols = setdiff(fac_cols, not_imp)

# creating a new column which contains unique combinations of all factors
eval(parse(text = 
  paste0("masked[, all_facs := paste(",
         paste0(fac_cols, collapse = ", "),
         ", sep = '__')]")
))

# keep all_facs with atleast 4 combinations in the test set. Rest can go in training! The idea is to make sure that test set doesn't encounter anything new while training set can have entries which occurred only once!
facs_N = masked[, .(all_facs_N = .N), all_facs]
masked = merge(masked, facs_N, by = "all_facs", all.x = T)

masked_complete   = masked[all_facs_N >= 4]
masked_incomplete = masked[all_facs_N < 4]

# Redo by reducing number of combinations (i.e. 4 above). Can't proceed!
rem_cols_1 = which(sapply(masked_complete, uniqueN) == 1) %>% names;
rem_cols_2 = which(sapply(masked_incomplete, uniqueN) == 1) %>% names;
if(length(rem_cols_1) + length(rem_cols_2) > 0) {
  stop("Redo!")
}


# train/test for H2O model
set.seed(1234);
# 80% for train and 20% for test
train_idx = createDataPartition(y = masked_complete$all_facs, p = 0.75)$Resample1;

masked_complete[,   `:=`(all_facs = NULL, all_facs_N = NULL)]
masked_incomplete[, `:=`(all_facs = NULL, all_facs_N = NULL)]

masked_train = masked_complete[train_idx]
masked_test  = masked_complete[-train_idx]
# add incomplete entries to train
masked_train_big = rbind(masked_train, masked_incomplete)

# write train/validate/test set  
fwrite(masked_train, "masked/train.csv")
fwrite(masked_test,  "masked/test.csv")
fwrite(masked_train_big, "masked/train_big.csv")

# also store category levels on which training is done!
# small
cols = setdiff(fac_cols, "Error_Status")
l = lapply(cols, function(c) masked_train[, unique(get(c))])
names(l) = cols
saveRDS(l, "masked/levels.RData")
# big
cols = setdiff(fac_cols, "Error_Status")
l = lapply(cols, function(c) masked_train_big[, unique(get(c))])
names(l) = cols
saveRDS(l, "masked/levels_big.RData")

# remove un-necessary columns from data
masked[, `:=`(all_facs = NULL, geography_code = NULL, all_facs_N = NULL)]


