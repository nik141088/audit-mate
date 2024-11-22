library(data.table)

process_dates = function(raw_data) {
  
  # invoice_date, scan_date and stamp_date
  raw_data[, invoice_date := as.Date(invoice_date, format = "%d-%m-%Y")]
  raw_data[, stamp_date := as.POSIXct(stamp_date, format = "%d-%m-%Y %H:%M", tz = "UTC")]
  raw_data[, scan_date := as.POSIXct(scan_date, format = "%d-%m-%Y %H:%M", tz = "UTC")]
  
  # scanning must happen after invoice is generated AND stamping (i.e. processing) must happen after scanning
  raw_data = raw_data[scan_date >= invoice_date]
  raw_data = raw_data[stamp_date >= scan_date]
  raw_data[, inv_to_scan := difftime(scan_date, invoice_date, unit = "day") %>% as.numeric];
  raw_data[, scan_to_stamp := difftime(stamp_date, scan_date, unit = "day") %>% as.numeric];
  
  # invoice DayofWeek, MonthofYear, WeekofYear, yearmon
  raw_data[, inv.DayofWeek   := weekdays(invoice_date)]
  # raw_data[, inv.WeekofYear  := lubridate::week(invoice_date)]
  raw_data[, inv.MonthofYear := month.name[lubridate::month(invoice_date)]]
  raw_data[, inv.Year        := lubridate::year(invoice_date) %>% as.character]
  
  # scan    DayofWeek, WeekofYear, yearmon, hour, minute
  raw_data[, scan.DayofWeek    := weekdays(scan_date)]
  raw_data[, scan.MonthofYear  := month.name[lubridate::month(scan_date)]]
  raw_data[, scan.Year         := lubridate::year(scan_date) %>% as.character]
  raw_data[, scan.hourofDay    := lubridate::hour(scan_date)]
  raw_data[, scan.minofHour    := (lubridate::minute(scan_date) / 10) %>% as.integer]
  raw_data[, scan.minofHour    := paste0(10*scan.minofHour, "-", 10*(scan.minofHour+1))]
  # raw_data[, scan.timeofDay    := scan.hourofDay + (scan.minofHour/60)]
  # raw_data[, scan.timeofDay.sq := scan.timeofDay^2]
  # raw_data[, `:=`(scan.hourofDay = NULL, scan.minofHour = NULL)]
  
  # stamp   DayofWeek, WeekofYear, yearmon, hour, minute
  raw_data[, stamp.DayofWeek    := weekdays(stamp_date)]
  raw_data[, stamp.MonthofYear  := month.name[lubridate::month(stamp_date)]]
  raw_data[, stamp.Year         := lubridate::year(stamp_date) %>% as.character]
  raw_data[, stamp.hourofDay    := lubridate::hour(stamp_date)]
  raw_data[, stamp.minofHour    := (lubridate::minute(stamp_date) / 10) %>% as.integer]
  raw_data[, stamp.minofHour    := paste0(10*stamp.minofHour, "-", 10*(stamp.minofHour+1))]
  # raw_data[, stamp.timeofDay    := stamp.hourofDay + (stamp.minofHour/60)]
  # raw_data[, stamp.timeofDay.sq := stamp.timeofDay^2]
  # raw_data[, `:=`(stamp.hourofDay = NULL, stamp.minofHour = NULL)]
  
  # remove invoice_date, scan_date and stamp_date
  raw_data[, `:=`(invoice_date = NULL, scan_date = NULL, stamp_date = NULL)]
  
  return(raw_data)
}
