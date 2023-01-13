## Create function to make grid, calculate metrics for different resolution grid sizes
# Written by Matt Biddle for h3 instead of dggridr

res_changes <- function(occ, resolution = 2, esn = 50){
  
  hex <- obisindicators::make_hex_res(resolution)
  
  occ <- occ %>%
    mutate(
      cell = h3::geo_to_h3(
        data.frame(decimallatitude,decimallongitude), #changed capitalization
        res = resolution
      )
    )
  
  idx <- obisindicators::calc_indicators(occ, esn = esn)
  
  grid <- hex %>%
    inner_join(
      idx,
      by = c("hexid" = "cell")
    )
} 