# Required libs
library(zoo);
library(dplyr);
library(ggplot2);
library(data.table);
library(lubridate);
library(stringr);
library(caret);

setwd('C:/Users/nikhi/Dropbox/GitHub/audit-mate')
source('cost_centre_mapping.R')
source('process_dates_and_timestamps.R')
source('masking.R')


options(scipen = 999)
raw_data = list.files("raw", "*", full.names = T) %>% lapply(fread, na.strings = "#N/A") %>% rbindlist

# change non alpha-numeric characters to underscores
names(raw_data) = str_replace_all(names(raw_data), "[^[:alnum:]]+", "_")
names(raw_data) = str_replace_all(names(raw_data), "[\\_]+$", "")
setnames(raw_data, "5_Reason_codes", "FIVE_Reason_codes")
setnames(raw_data, "5_Rejection_count", "FIVE_Rejection_count")
setnames(raw_data, "5_Reason_Code_Accuracy", "FIVE_Reason_Code_Accuracy")

# keep relevant columns only
# either use Country or the tuple .(Region, Establishment). Former will be more accurate
# Error_Status is "Correct Data" for ReasonCode %in% c("No Error", "Invoice not to be paid", "Incorrect\\insufficient instruction given to BUD"); for other ReasonCodes it is "Error"
raw_data = raw_data[, .(doc_id, Error_Status, ReasonCode, comp_no, VendorID, invoice_date, scan_date, stamp_date, invoice_sum, invoice_currency, log_index, UserID, DocumentType, CostCenter, Country, Market, User, Status_Index)]

# date and timestamps
raw_data = process_dates(raw_data)

# save masks (part 1)
save_masks(raw_data, cols = c('VendorName', 'invoice_currency', 'UserID', 'Description', 'PrinDeptCode', 'log_comment', 'Country', 'Market', 'User', 'Region', 'Country_Name', 'Europe_Non_Europe', 'Establishment', 'Focus_Vendors'))



# converting some integers to characters. These should be factors
raw_data[, log_index := as.character(log_index)]

# keep only unique entries
raw_data = unique(raw_data)

# numeric error status for plotting purposes
raw_data[, Error_Status_numeric := 0]
raw_data[Error_Status == "Error", Error_Status_numeric := 1]



# Further processing:
# comp_no mapping into client and geography (comp_no_2)
# CostCenter mapping into Overall, Function and Location
# get unique comp_no and CostCenter
comp_no_Unique = raw_data[, .(comp_no = unique(comp_no))]
CostCenter_Unique = raw_data[, .(CostCenter = unique(CostCenter))]
# save for future use
fwrite(comp_no_Unique, "map/comp_no_unqiue.csv")
fwrite(CostCenter_Unique, "map/cost_centre_unqiue.csv")

# comp_no mapping
comp_no_map = compute_comp_no_map(comp_no_Unique)
# mapping aidrport codes and extracting info from cost centres
CostCenter_map = compute_cost_centre_map(CostCenter_Unique)

# save for future use
fwrite(comp_no_map,    "map/comp_no_map.csv")
fwrite(CostCenter_map, "map/CostCenter_map.csv")

# add mapping to raw_data
raw_data = merge(raw_data, comp_no_map, by = "comp_no", all.x = T)
raw_data = merge(raw_data, CostCenter_map, by = "CostCenter", all.x = T)

# save masks (part 2)
save_masks(raw_data, cols = c('CostCenter', 'comp_no', 'client', 'geography_code', 'CostCenter.Location', 'CostCenter.Overall', 'CostCenter.Function', 'CostCenter.Location.Type', 'CostCenter.Location.Name'))

# only keep the rows where mapping is found
raw_data = raw_data[!is.na(CostCenter.Location) & !is.na(CostCenter.Overall) & !is.na(CostCenter.Function)]


# save
fwrite(raw_data, 'unmasked/data.csv')
