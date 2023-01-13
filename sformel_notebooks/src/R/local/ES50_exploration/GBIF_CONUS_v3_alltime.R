#Follow up script to: make_CONUS_demo_maps_for_USGS_mgmt.R
#Implementing improvements tested in: Test_parallel_and_function_improvement.R


#z <- seq(from = 1960, to = 2022, by = 10)

#Much faster when run inside lapply and outside of Rstudio.
z <- 1960

lapply(z, function(z){
  
  #Set Parameters-----
  #What resolutions to run?
  res <- c(1:5)
  
  #At what sampling depth?
  esn <- 50
  
  #Where to output images? What resolution?
  #Be aware that map elements will change disproportionally with resolution.
  
  subfolder <- paste0("output/tests/ES", esn, "_CONUS")
  
  px_width <- 6400
  px_height <- 3200
  
  #Define decade
  x <- z  #for testing, eventually need to figure out how to loop through decades
  
  #Set timespan: Create list of queries by decade
  decades <- seq(from = x, to = 2022, by = 10)
  
  #Define some functions-----
  
  #states
  USA_shape <- rnaturalearth::ne_states(returnclass = "sf", country = "United States of America")
  world <- rnaturalearth::ne_countries(country = "United States of America", scale = "small", returnclass = "sf")
  
  #Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them
  
  #partly based on John Waller's plots: https://github.com/jhnwllr/es50
  
  #bounding box for Contiguous US (CONUS) + buffer
  West_Bounding_Coordinate <- -124.211606 - 0.5
  East_Bounding_Coordinate <- -67.158958 - 0.5
  North_Bounding_Coordinate <- 49.384359 - 0.5
  South_Bounding_Coordinate <- 25.837377 - 0.5
  
  bb_roi <- paste('POLYGON ((', 
                  West_Bounding_Coordinate,South_Bounding_Coordinate, ",", 
                  East_Bounding_Coordinate, South_Bounding_Coordinate, ",",
                  East_Bounding_Coordinate, North_Bounding_Coordinate, ",",
                  West_Bounding_Coordinate, North_Bounding_Coordinate, ",",
                  West_Bounding_Coordinate, South_Bounding_Coordinate, '))')
  
  gmap_discrete <- function(grid,
                            column = "shannon",
                            label = "Shannon index",
                            trans = "identity",
                            crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                            limits = c(0, 8)) {
    
    
    sfc <- st_as_sfc(bb_roi, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
    bb <- sf::st_bbox(st_transform(sfc, crs))
    
    breaks = seq(min(limits), max(limits), max(limits)/5)
    
    grid[[column]] <- cut(grid[[column]], breaks = breaks)
    
    ggplot() +
      
      geom_sf(
        data = grid,
        aes_string(fill = column, geometry = "geometry"),
        show.legend = TRUE,
        lwd = 0
      ) + 
      scale_fill_brewer(palette = "Spectral", direction=-1) +
      
      geom_sf(
        data = world,
        fill = NA,
        color = "grey",
        lwd = 0.1
      ) +
      
      geom_sf(
        data = USA_shape,
        fill = NA,
        color = "#000000",
        lwd = 0.1
      ) +
      
      coord_sf(crs  = crs,
               xlim = bb[c("xmin", "xmax")],
               ylim = bb[c("ymin", "ymax")]) +
      
      theme(
        panel.background = element_rect(
          fill = "gray95",
          colour = "gray95",
          size = 0,
          linetype = "blank"
        ),
        axis.ticks = element_blank(),
        #axis.title.x = element_blank(),
        #axis.title.y = element_blank(),
        #axis.text = element_blank(),
        #panel.grid = element_blank(),
        plot.margin = margin(0, 0, 0, 0, "cm")
      )
  }
  
  #Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them
  gmap <- function(grid,
                   column = "shannon",
                   label = "Shannon index",
                   trans = "identity",
                   crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                   limits = c(0, 8)) {
    
    sfc <- st_as_sfc(bb_roi, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
    bb <- sf::st_bbox(st_transform(sfc, crs))
    
    ggplot() +
      
      geom_sf(
        data = grid,
        aes_string(fill = column, geometry = "geometry"),
        show.legend = TRUE,
        lwd = 0
      ) +
      
      viridis::scale_fill_viridis(
        option = "inferno",
        na.value = "white",
        name = label,
        trans = trans,
        limits = limits
      ) +
      
      geom_sf(
        data = world,
        fill = NA,
        color = "grey",
        lwd = 0.1
      ) +
      
      geom_sf(
        data = USA_shape,
        fill = NA,
        color = "#000000",
        lwd = 0.1
      ) +
      
      coord_sf(crs  = crs,
               xlim = bb[c("xmin", "xmax")],
               ylim = bb[c("ymin", "ymax")]) +
      
      theme(
        panel.background = element_rect(
          fill = "gray95",
          colour = "gray95",
          size = 0,
          linetype = "blank"
        ),
        axis.ticks = element_blank(),
        #axis.title.x = element_blank(),
        #axis.title.y = element_blank(),
        #axis.text = element_blank(),
        #panel.grid = element_blank(),
        plot.margin = margin(0, 0, 0, 0, "cm")
      )
  }
  
  tictoc::tic()
  #load libraries and functions----
  
  library(obisindicators)
  library(readr)
  library(dplyr)
  library(sf)
  library(ggplot2)
  
  source("src/make_hex_res_parallel.R")
  source("src/obis_calc_indicators_improved_dt.R")
  source("src/gmap_discrete.R")
  source("src/gmap.R")
  
  #Read in data-----
  
  tictoc::tic()
  
  #list CSV files generated on USGS HPC
  
  lf <- list.files("data/processed/gbif_usa_1969_2022/", pattern = "*.csv", full.names = TRUE)
  #lf <- list.files("/caldera/projects/css/sas/bio/spec_obs/gbif/output/", pattern = "^gbif_USA_.*.csv", full.names = TRUE)
  
  #make list for map
  dec_trimmed <- strtrim(decades, 3)
  
  #read in csv made in HPC and filter to bounding box of CONUS, remove unnecessary columns
  occ_by_decade <- lapply(dec_trimmed, function(x){
    file_names <- lf[grepl(lf, pattern = paste0("gbif_USA_",x,"\\d.csv"))]
    do.call(rbind, lapply(file_names, function(x){
      data.table::fread(x) %>%
        select(-kingdom, -class) %>% 
        filter(!is.na(decimallongitude)) %>% 
        filter(!is.na(species)) %>% 
        filter(decimallatitude < North_Bounding_Coordinate) %>% 
        filter(decimallatitude > South_Bounding_Coordinate) %>%
        filter(decimallongitude < East_Bounding_Coordinate) %>%
        filter(decimallongitude > West_Bounding_Coordinate)
    }))
  })
  
  #add decades as names
  names(occ_by_decade) <- decades
  
  time_setup <- tictoc::toc()
  
  #Index data points to h3----
  #What is returned is the list of h3 indices for each row in occurrences
  
  
  tictoc::tic()
  
  occ <- occ_by_decade %>%
    data.table::rbindlist(idcol = "decade") %>% 
    select(decimallatitude,
           decimallongitude)
  
  cells <- lapply(res, function(x){
    h3::geo_to_h3(latlng = occ,
                  res = x)
  })
  
  time_spatial_index_of_data <- tictoc::toc()
  
  names(cells) <- paste0("res_", res)
  
  #Make global h3 grids-----
  #(res 6 takes 30 min, make once, save as an RDS and import).
  # resolution 1 to 5 takes about 2.5 min on my laptop.
  
  
  #h3_grids <- lapply(res, make_h3_grids_parallel)
  
  #read in saved versions of the grids to save time
  h3_grids <- readRDS("output/h3_grids/h3_res_1to5_globalgrid.rds")[res]
  
  if(any(res %in% c(6))){
    h3_grids[6] <- readRDS("output/h3_grids/h3_res6_globalgrid.rds")
  }
  
  
  #calculate diversity indices----
  tictoc::tic()
  
  occ <-occ <- occ_by_decade %>%
  data.table::rbindlist(idcol = "decade")
  
  bio_idx <- lapply(cells, function(x){
    
    occ <- cbind(occ, "cell" = unlist(x))
    
    occ %>%
      select(-decimallatitude, -decimallongitude,) %>% 
      obis_calc_indicators_improved_dt(esn = esn)
    
  })
  
  time_diversity_cal <- tictoc::toc()
  
  #join grid and indicators----
  grid <- purrr::map2(h3_grids, bio_idx,function(x,y){
    x %>%
      inner_join(
        y,
        by = c("hex_ids" = "cell"))
  })
  
  #Plot and save all resolutions and metrics----
  
  x <- "1960-2022"
  
  tictoc::tic()
  
  lapply(res, function(y){
    
    # map defaults
    grid <- grid[[y]]  #This breaks down when res doesn't xtart at 1
    RES <- y # defined at https://h3geo.org/docs/core-library/restable/
    column <- "es"
    label <- paste0("ES", esn) #legend label
    trans <- "identity"
    crs <- "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
    limits <- c(0,esn) #legend limits
    
    # make map
    map <- gmap(
      grid,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits)
    
    map 
    
    ggsave(
      paste0(subfolder,"/", label, "_CONUS_", x, "_res_", RES, ".png"),
      plot = map,
      width = px_width,
      height = px_height,
      units = "px",
      scale = 1,
      limitsize = FALSE)
    
    #Make Shannon Maps
    
    label <- "shannon"
    column <- "shannon"
    limits <- c(0,10)
    # make map
    map <- gmap(
      grid,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits)
    
    map 
    
    ggsave(
      paste0(subfolder,"/", column, "_CONUS_", x, "_res_", RES, ".png"),
      plot = map,
      width = px_width,
      height = px_height,
      units = "px",
      scale = 1,
      limitsize = FALSE
    )
    
    #Make Hill 1 Maps
    
    label <- "hill_1"
    column <- "hill_1"
    limits <- c(0, max(grid$hill_1))
    # make map
    map <- gmap(
      grid,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits)
    
    map 
    
    ggsave(
      paste0(subfolder,"/", column, "_CONUS_", x, "_res_", RES, ".png"),
      plot = map,
      width = px_width,
      height = px_height,
      units = "px",
      scale = 1,
      limitsize = FALSE
    )
  })
  
  #Make binned colors for diversity----
  
  lapply(res, function(y){
    
    # map defaults
    grid <- grid[[y]]
    RES <- y # defined at https://h3geo.org/docs/core-library/restable/
    column <- "es"
    label <- paste0("ES", esn) #legend label
    trans <- "identity"
    crs <- "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
    limits <- c(0,esn) #legend limits
    
    # make map
    map <- gmap_discrete(
      grid,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits)
    
    map 
    
    ggsave(
      paste0(subfolder,"/", label, "_CONUS_", x, "_res_", RES,"_discrete", ".png"),
      plot = map,
      width = px_width,
      height = px_height,
      units = "px",
      scale = 1,
      limitsize = FALSE)
    
    #Make Shannon Maps
    
    label <- "shannon"
    column <- "shannon"
    limits <- c(0,10)
    # make map
    map <- gmap_discrete(
      grid,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits)
    
    map 
    
    ggsave(
      paste0(subfolder,"/", column, "_CONUS_", x, "_res_", RES,"_discrete", ".png"),
      plot = map,
      width = px_width,
      height = px_height,
      units = "px",
      scale = 1,
      limitsize = FALSE
    )
    
    #Make Hill 1 Maps
    
    label <- "hill_1"
    column <- "hill_1"
    limits <- c(0, max(grid$hill_1))
    # make map
    map <- gmap_discrete(
      grid,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits)
    
    map 
    
    ggsave(
      paste0(subfolder,"/", column, "_CONUS_", x, "_res_", RES,"_discrete", ".png"),
      plot = map,
      width = px_width,
      height = px_height,
      units = "px",
      scale = 1,
      limitsize = FALSE
    )
  })
  
  
  time_plot <- tictoc::toc()
  
  time_total_run <- tictoc::toc()
  
  #Write file of runtime information----
  
  #Name of computer
  computer_name <- "SFORMEL GFE"
  
  runlist <- ls(pattern = "^time")
  
  names(runlist) <- ls(pattern = "^time")
  
  runlist <- lapply(runlist, function(x){
    x %>% 
      get %>% 
      .['callback_msg'] %>% 
      unlist() %>% 
      unname()
  }) %>% 
    unlist()
  
  runlist <- append(runlist, c(computer_name))
  names(runlist)[6] <- c("computer_name") 
  
  write.table(x = runlist, 
              file = paste0(subfolder,"/", paste0("ES", esn), "runtimes_", x, "_res_", paste(min(res), max(res), sep = "-"), ".txt"),
              col.names = FALSE)
  
  #store vectors to compare metrics
  write_rds(bio_idx, file = paste0(subfolder,"/", paste0("ES", esn), x, "_res_", paste(min(res), max(res), sep = "-"), "_bioidx.rds"))
  
})