library(data.table)

find_mask_for_unique_entries = function(dt, column) {
  col_map = dt[, .(unique_val = unique(get(column)))]
  col_map[, masked_val := paste0(column, '_', .I)]
  
  return(col_map)
}

save_masks = function(dt, cols) {
  for (c in cols) {
    print(c)
    col_map = find_mask_for_unique_entries(dt, c)
    fwrite(col_map, paste0('map/masking/', c, '_map.csv'))
  }
}

get_all_masking_columns = function() {
  masking_maps = list.files("map/masking", "*", full.names = F)
  masking_maps = stringr::str_replace_all(masking_maps, '_map\\.csv$', '')
  return(masking_maps)
}

apply_mask = function(dt, cols = NULL) {
  if(is.null(cols)) {
    cols = get_all_masking_columns()
  }
  for (col in cols) {
    masked_col = paste0(col, '_masked')
    if(col %in% names(dt)) {
      print(col)
      # read masking map
      map_dt = fread(paste0('map/masking/', col, '_map.csv'))
      names(map_dt) = c(col, masked_col)
      dt = merge(dt, map_dt, by = col, all.x = T)
      dt[, (col) := NULL]
      # rename column masked_col to col
      setnames(dt, masked_col, col)
    }
  }
  return(dt)
}

remove_mask = function(dt, cols = NULL) {
  if(is.null(cols)) {
    cols = get_all_masking_columns()
  }
  for (col in cols) {
    unmasked_col = paste0(col, '_unmasked')
    if(col %in% names(dt)) {
      print(col)
      # read masking map
      map_dt = fread(paste0('map/masking/', col, '_map.csv'))
      names(map_dt) = c(unmasked_col, col)
      dt = merge(dt, map_dt, by = col, all.x = T)
      dt[, (col) := NULL]
      # rename column unmasked_col to col
      setnames(dt, unmasked_col, col)
    }
  }
  return(dt)
}
