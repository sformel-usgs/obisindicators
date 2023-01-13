#This breaks out a part of the res_changes function made by Matt Biddle so it can be parallel

make_h3_grids_parallel <- function (hex_res = 2){
  CRS <- sf::st_crs(4326)
  east <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 0,
      xmax = 180,
      ymin = -90,
      ymax = 90
    ), crs = CRS
  )))
  west <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = -180,
      xmax = 0,
      ymin = -90,
      ymax = 90
    ), crs = CRS
  )))
  hex_ids <-
    c(h3::polyfill(east, res = hex_res),
      h3::polyfill(west,
                   res = hex_res))
  dl_offset <- 60
  
  future::plan(strategy = "multisession",
               workers = future::availableCores() * 0.75)
  
  #make sliding chunk size with res
  chunk <- dplyr::case_when(hex_res == 4 ~ 2e4,
                            hex_res == 5 ~ 5e4,
                            hex_res == 6 ~ 1e5,
                            TRUE ~ 1e4)
  n <- length(hex_ids)
  r  <- rep(1:ceiling(n / chunk), each = chunk)[1:n]
  hex_ids <- split(hex_ids, r)
  
  hex_sf <-
    furrr::future_map_dfr(hex_ids, function(x) {
      h3::h3_to_geo_boundary_sf(unlist(x))
    },
    .options = furrr::furrr_options(seed = NULL)) %>%
    sf::st_as_sf(data.table::rbindlist(.)) %>%
    sf::st_wrap_dateline(c(
      "WRAPDATELINE=YES",
      glue::glue("DATELINEOFFSET={dl_offset}")
    )) %>%
    dplyr::rename(hex_ids = h3_index)
  
  future::plan(strategy = "sequential")
  
  return(hex_sf)
}
