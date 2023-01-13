#Index each h3 polygon to a hemisphere so the data can be further partitioned for coputing on smaller achines.

#h3 can't polyfill beyond 180 degree arcs
library(dplyr)
  
CRS <- sf::st_crs(4326)
  
  NW1 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 0,
      xmax = -90,
      ymin = 0,
      ymax = 90
    ), crs = CRS
  )))
  
  NW2 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = -90,
      xmax = -180,
      ymin = 0,
      ymax = 90
    ), crs = CRS
  )))
  
  NW_hex_ids <- c(h3::polyfill(NW1, res = 5),
                  h3::polyfill(NW2, res = 5))
  
  
  NE1 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 0,
      xmax = 90,
      ymin = 0,
      ymax = 90
    ), crs = CRS
  )))
  
  NE2 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 90,
      xmax = 180,
      ymin = 0,
      ymax = 90
    ), crs = CRS
  )))
  
  NE_hex_ids <- c(h3::polyfill(NE1, res = 5),
                  h3::polyfill(NE2, res = 5))
  
  
  SW1 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 0,
      xmax = -90,
      ymin = 0,
      ymax = -90
    ), crs = CRS
  )))
  
  SW2 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = -90,
      xmax = -180,
      ymin = 0,
      ymax = -90
    ), crs = CRS
  )))
  
  SW_hex_ids <- c(h3::polyfill(SW1, res = 5),
                  h3::polyfill(SW2, res = 5))
  
  SE1 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 0,
      xmax = 90,
      ymin = 0,
      ymax = -90
    ), crs = CRS
  )))
  
  SE2 <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 90,
      xmax = 180,
      ymin = 0,
      ymax = -90
    ), crs = CRS
  )))
  
  SE_hex_ids <- c(h3::polyfill(SE1, res = 5),
                  h3::polyfill(SE2, res = 5))
  
  
  # dl_offset <- 60
  # 
  # 
  # Q <- h3::h3_to_geo_boundary_sf(NW_hex_ids) %>%
  # sf::st_as_sf(data.table::rbindlist(.)) %>%
  # sf::st_wrap_dateline(c(
  #   "WRAPDATELINE=YES",
  #   glue::glue("DATELINEOFFSET={dl_offset}")
  # )) %>%
  # dplyr::rename(hex_ids = h3_index)
