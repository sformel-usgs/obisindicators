#ES50 calculated for United STates on USGS HPC Denali
#Based off original code from Abby Benson: use_gbif_parquet_file.rmd

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

#This helped: https://data-blog.gbif.org/post/shapefiles/

# We kept running into memory limitations. While we're figuring that out, we decided to focus on maps that showed decades in the CONUS for discussion with USGS management.

library(obisindicators)
library(readr)
library(dplyr)
library(sf)
#library(arrow)
#library(magick)
library(ggplot2)

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


output_dir <- "/caldera/projects/css/sas/bio/spec_obs/gbif/output/"

#Create list of queries by decade
decades <- seq(from = 1960, to = 2022, by = 10)


#get US states and territories from natural earth.  See this for my inspiration: https://github.com/kguidonimartins/misc/blob/main/R/quick_map.R
USA_shape <-
  rnaturalearth::ne_countries(scale = "large", returnclass = "sf") %>%
  dplyr::filter_all(., dplyr::any_vars(stringr::str_detect(
    ., paste("United States of America", collapse = "|")
  )))

#list CSV files generated on USGS HPC
lf <- list.files("/caldera/projects/css/sas/bio/spec_obs/gbif/output/", pattern = "^gbif_USA_.*.csv", full.names = TRUE)

#make list for map
dec_trimmed <- strtrim(decades, 3)


#bounding box for Contiguous US (CONUS)

West_Bounding_Coordinate <- -124.211606
East_Bounding_Coordinate <- -67.158958
North_Bounding_Coordinate <- 49.384359
South_Bounding_Coordinate <- 25.837377

tictoc::tic()

#read in csv made in HPC and filter to bounding box of CONUS, remove unnecessary columns
occ_by_decade <- lapply(dec_trimmed, function(x) {
  file_names <-
    lf[grepl(lf, pattern = paste0("gbif_USA_", x, "\\d.csv"))]
  do.call(rbind, lapply(file_names, function(x) {
    read_csv(x) %>%
      select(-kingdom,-class) %>%
      filter(!is.na(decimallongitude)) %>%
      filter(!is.na(species)) %>%
      filter(decimallatitude < North_Bounding_Coordinate) %>%
      filter(decimallatitude > South_Bounding_Coordinate) %>%
      filter(decimallongitude < East_Bounding_Coordinate) %>%
      filter(decimallongitude > West_Bounding_Coordinate)
  }))
})

tictoc::toc()

#add decades as names
names(occ_by_decade) <- decades

#Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them

world <-
  rnaturalearth::ne_countries(country = "United States of America",
                              scale = "small",
                              returnclass = "sf")

bb_roi <-
  paste(
    'POLYGON ((',
    West_Bounding_Coordinate,
    South_Bounding_Coordinate,
    ",",
    East_Bounding_Coordinate,
    South_Bounding_Coordinate,
    ",",
    East_Bounding_Coordinate,
    North_Bounding_Coordinate,
    ",",
    West_Bounding_Coordinate,
    North_Bounding_Coordinate,
    ",",
    West_Bounding_Coordinate,
    South_Bounding_Coordinate,
    '))'
  )

gmap <- function(grid,
                 column = "shannon",
                 label = "Shannon index",
                 trans = "identity",
                 crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                 limits = c(0, 8)) {
  sfc <-
    st_as_sfc(bb_roi, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
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

RES_list <- c(3:4)

tictoc::tic()
lapply(as.character(decades), function(x) {
  lapply(RES_list, function(y) {
    # map defaults
    RES <-
      y # defined at https://h3geo.org/docs/core-library/restable/
    esn <- 50
    column <- "es"
    label <- paste0("ES", esn)
    trans <- "identity"
    crs <-
      "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
    limits <- c(0, esn)
    
    # make grid
    grid_dec <-
      res_changes(occ = occ_by_decade[x][[1]],
                  # a little silly, but this lets me call the data frame by name and use the name in the file output
                  resolution = RES,
                  esn = esn)
    
    # make map
    map <- gmap(
      grid_dec,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits
    ) +
      labs(caption =)
    
    map
    
    ggsave(
      paste0(
        output_dir, 
        "/CONUS/",
        label,
        "CONUS_",
        x,
        "_res_",
        RES,
        ".png"
      ),
      plot = map,
      width = 1600,
      height = 800,
      units = "px",
      scale = 1,
      limitsize = FALSE
    )
  })
  
})

tictoc::toc()

#Res 1 and 2 took ~ 16 minutes on a Denali node.
#Res 3 and 4 took 1 hour and 48 minutes

#Process all decades ----

library(obisindicators)
library(readr)
library(dplyr)
library(sf)
library(ggplot2)

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


output_dir <- "/caldera/projects/css/sas/bio/spec_obs/gbif/output/"

#Create list of queries by decade
decades <- seq(from = 1960, to = 2022, by = 10)


#get US states and territories from natural earth.  See this for my inspiration: https://github.com/kguidonimartins/misc/blob/main/R/quick_map.R
USA_shape <-
  rnaturalearth::ne_countries(scale = "large", returnclass = "sf") %>%
  dplyr::filter_all(., dplyr::any_vars(stringr::str_detect(
    ., paste("United States of America", collapse = "|")
  )))

#list CSV files generated on USGS HPC
lf <- list.files("/caldera/projects/css/sas/bio/spec_obs/gbif/output/", pattern = "^gbif_USA_.*.csv", full.names = TRUE)

#make list for map
dec_trimmed <- strtrim(decades, 3)


#bounding box for Contiguous US (CONUS) + buffer

West_Bounding_Coordinate <- -124.211606 - 0.5
East_Bounding_Coordinate <- -67.158958 - 0.5
North_Bounding_Coordinate <- 49.384359 + 0.5
South_Bounding_Coordinate <- 25.837377 + 0.5

tictoc::tic()

#read in csv made in HPC and filter to bounding box of CONUS, remove unnecessary columns
occ_by_decade <- lapply(dec_trimmed, function(x) {
  file_names <-
    lf[grepl(lf, pattern = paste0("gbif_USA_", x, "\\d.csv"))]
  do.call(rbind, lapply(file_names, function(x) {
    read_csv(x) %>%
      select(-kingdom,-class) %>%
      filter(!is.na(decimallongitude)) %>%
      filter(!is.na(species)) %>%
      filter(decimallatitude < North_Bounding_Coordinate) %>%
      filter(decimallatitude > South_Bounding_Coordinate) %>%
      filter(decimallongitude < East_Bounding_Coordinate) %>%
      filter(decimallongitude > West_Bounding_Coordinate)
  }))
})

tictoc::toc()

#add decades as names
names(occ_by_decade) <- decades

#Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them

world <-
  rnaturalearth::ne_countries(country = "United States of America",
                              scale = "small",
                              returnclass = "sf")

bb_roi <-
  paste(
    'POLYGON ((',
    West_Bounding_Coordinate,
    South_Bounding_Coordinate,
    ",",
    East_Bounding_Coordinate,
    South_Bounding_Coordinate,
    ",",
    East_Bounding_Coordinate,
    North_Bounding_Coordinate,
    ",",
    West_Bounding_Coordinate,
    North_Bounding_Coordinate,
    ",",
    West_Bounding_Coordinate,
    South_Bounding_Coordinate,
    '))'
  )

gmap <- function(grid,
                 column = "shannon",
                 label = "Shannon index",
                 trans = "identity",
                 crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                 limits = c(0, 8)) {
  sfc <-
    st_as_sfc(bb_roi, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
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

RES_list <- c(1:4)

tictoc::tic()
  lapply(RES_list, function(y) {
    # map defaults
    RES <- y # defined at https://h3geo.org/docs/core-library/restable/
    esn <- 50
    column <- "es"
    label <- paste0("ES", esn)
    trans <- "identity"
    crs <-
      "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
    limits <- c(0, esn)
    
    # make grid
    grid_dec <-
      res_changes(data.table::rbindlist(occ_by_decade),
                  resolution = RES,
                  esn = esn)
    
    # make map
    map <- gmap(
      grid_dec,
      column,
      label = label,
      trans = trans,
      crs = crs,
      limits = limits
    ) +
      labs(caption =)
    
    map
    
    ggsave(
      paste0(
        output_dir, 
        "/CONUS/",
        label,
        "CONUS_1960-2022_res_",
        RES,
        ".png"
      ),
      plot = map,
      width = 1600,
      height = 800,
      units = "px",
      scale = 1,
      limitsize = FALSE
    )
  })

tictoc::toc()

