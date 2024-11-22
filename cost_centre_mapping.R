library(data.table)
library(stringr)
source('dpm_map.R')


compute_comp_no_map = function(comp_no_Unique) {
  comp_no_map = copy(comp_no_Unique)
  comp_no_map[, client := str_extract(comp_no, "^[A-Za-z]+")]
  comp_no_map[, geography_code := str_extract(comp_no, "[0-9]+$")]
  return(comp_no_map)
}

compute_cost_centre_map = function(CostCenter_Unique) {
  
  # CostCenter mapping
  CostCenter_map = copy(CostCenter_Unique)
  # remove space slash space any characters
  CostCenter_map[, CostCenter.New := str_replace(CostCenter, "[ ]*/[ ]*.*$", "")]
  # remove space hyphen space any characters
  CostCenter_map[, CostCenter.New := str_replace(CostCenter.New, "[ ]*-[ ]*.*$", "")]
  # only keep the ones having all caps characters ONLY. There should be 5 to 7 chars only
  CostCenter_map[, keep := str_detect(CostCenter.New, "^[A-Z]{5,7}$")]
  CostCenter_map[keep == FALSE, CostCenter.New := ""]
  CostCenter_map[, keep := NULL];
  # Function/Airport convention
  CostCenter_map[, CostCenter.Overall  := ""]
  CostCenter_map[, CostCenter.Function := ""]
  CostCenter_map[, CostCenter.Location := ""]
  
  dpm = get_dpm_map()
  
  dpm[, loc.start := nchar(pattern.left) + 1]
  dpm[, loc.end   := loc.start + 2]
  
  dpm[, regex_pattern := paste0("^", pattern.left, "[A-Z]{3,3}", pattern.right, "$")]
  
  # apply dpm rules to CostCenter
  for(i in 1:nrow(dpm)) {
    # print(i)
    CostCenter_map[, keep := FALSE]
    CostCenter_map[, keep := str_detect(CostCenter.New, dpm$regex_pattern[i])]
    CostCenter_map[keep == TRUE,
                   CostCenter.Overall  := paste0(CostCenter.Overall,  dpm$CostCenter.Overall[i])]
    CostCenter_map[keep == TRUE,
                   CostCenter.Function := paste0(CostCenter.Function, dpm$CostCenter.Function[i])]
    CostCenter_map[keep == TRUE,
                   CostCenter.Location := paste0(CostCenter.Location,
                                                 substr(CostCenter.New,
                                                        dpm$loc.start[i],
                                                        dpm$loc.end[i]))]
    CostCenter_map[, keep := NULL]
  }
  
  # add airport name (see https://datahub.io/core/airport-codes)
  airport_codes = fread("map/airport-codes.csv")
  airport_codes = airport_codes[CostCenter.Location.Type %in% paste0(c("small", "medium", "large"), "_airport")] %>% unique
  airport_codes = airport_codes[, .SD[1], .(CostCenter.Location, CostCenter.Location.Type)]
  
  CostCenter_map = merge(CostCenter_map, airport_codes, by = "CostCenter.Location", all.x = T)
  
  # discard empty values
  CostCenter_map = CostCenter_map[CostCenter.Overall != "" & CostCenter.Function != "" & CostCenter.Location != ""]
  CostCenter_map[, `:=`(CostCenter.New = NULL)]
  
  return(CostCenter_map)
}
