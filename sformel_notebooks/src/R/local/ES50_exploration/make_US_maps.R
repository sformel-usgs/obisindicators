#ES50 calculated for United STates on USGS HPC Denali
#Based off original code from Abby Benson: use_gbif_parquet_file.rmd

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

#This helped: https://data-blog.gbif.org/post/shapefiles/

library(obisindicators)
library(readr)
library(dplyr)
library(sf)
#library(arrow)
#library(magick)
library(ggplot2)
library(future) #If you don't load these explicitly you get socket errors
library(furrr) #If you don't load these explicitly you get socket errors
source("src/h3_grid.R")

#Need to make a table of non-US observations that are listed with US as countrycode

#Create list of queries by decade
decades <- seq(from = 1960, to = 1979, by = 10)


#get US states and territories from natural earth.  See this for my inspiration: https://github.com/kguidonimartins/misc/blob/main/R/quick_map.R
USA_shape <-
  rnaturalearth::ne_countries(scale = "large", returnclass = "sf") %>%
  dplyr::filter_all(., dplyr::any_vars(stringr::str_detect(., paste("United States of America", collapse = "|"))))

#Read in CSV files generated on USGS HPC

lf <- list.files("data/processed/gbif_usa_1969_2022/", pattern = "*.csv", full.names = TRUE)
#lf <- list.files("/caldera/projects/css/sas/bio/spec_obs/gbif/output/", pattern = "^gbif_USA_.*.csv", full.names = TRUE)

#make list for map
dec_trimmed <- strtrim(decades, 3)

#read in csv made in HPC
occ_by_decade <- lapply(dec_trimmed, function(x){
  file_names <- lf[grepl(lf, pattern = paste0("gbif_USA_",x,"\\d.csv"))]
  do.call(rbind, lapply(file_names, function(x){
    read_csv(x) %>%
    filter(!is.na(decimallongitude))
    }))
})

#convert to sf in parallel
tictoc::tic()

future::plan(strategy = "multisession", workers = future::availableCores()*0.75)

occ_by_decade <- lapply(occ_by_decade, function(y){
  
  #split df into 100,000 row chunks
  chunk <- 100000
  n <- nrow(y)
  r  <- rep(1:ceiling(n/chunk),each=chunk)[1:n]
  d <- split(y,r)
  
  
  furrr::future_map(d, function(x){
    x %>% 
      st_as_sf(coords = c("decimallongitude", "decimallatitude"),
               crs = "WGS84",
               agr = "identity")
  }, .options = furrr::furrr_options(seed = NULL)) %>% 
    data.table::rbindlist() %>% 
    sf::st_as_sf()
  
})

future::plan('sequential')
tictoc::toc()

#On my GFE, 1960 - 1999 took about: 200 sec
#Unable to run all years on GFE

#add decades as names
names(occ_by_decade) <- decades

#filter to USA polygons in parallel
tictoc::tic()

future::plan(strategy = "multisession", workers = future::availableCores()*0.75)

occ_by_decade_filtered <- lapply(occ_by_decade, function(y){

  #split df into 100,000 row chunks
  chunk <- 100000
  n <- nrow(y)
  r  <- rep(1:ceiling(n/chunk),each=chunk)[1:n]
  d <- split(as.data.frame(y), r)
  
  furrr::future_map(d, function(x){
  x %>% 
    filter(st_intersects(st_as_sf(x), USA_shape) %>% lengths > 0)
  }, .options = furrr::furrr_options(seed = NULL)) %>% 
    data.table::rbindlist() %>% 
    sf::st_as_sf()

})

future::plan('sequential')

tictoc::toc()

#On my GFE took about: 

#Plot function, based off one written by Matt Biddle

#Bounding Box for each polygon

# bboxes <- lapply(USA_shape$geometry, function(x){
#   x %>% 
#     st_bbox() %>% 
#     as.data.frame()
# }) %>%
#   data.table::rbindlist()

ggplot() +
  geom_polygon()

geom_rect(data=d, mapping=aes(xmin=x1, xmax=x2, ymin=y1, ymax=y2, fill=t), color="black", alpha=0.5)

gmap <- function(grid,
                 column = "shannon",
                 label = "Shannon index",
                 trans = "identity",
                 crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                 limits = c(0, 8)) {
  world <-
     rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
  bbglobe <- 'POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))'
  
  sfc <- st_as_sfc(bbglobe, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
  bb <- sf::st_bbox(st_transform(sfc, crs))
  ggplot() +
    
    geom_sf(
      data = grid,
      aes_string(fill = column, geometry = "geometry"),
      show.legend = TRUE,
      lwd = 0
    ) +
    
    # viridis::scale_color_viridis(
    #   option = "inferno",
    #   na.value = "white",
    #   name = label,
    #   trans = trans,
    #   limits = limits
    # ) +

    viridis::scale_fill_viridis(
      option = "inferno",
      na.value = "white",
      name = label,
      trans = trans,
      limits = limits
    ) +
    
    geom_sf(data = world,
            fill = NA,
            color = "grey",
            lwd = 0.1) +
    
    geom_sf(data = USA_shape,
            fill = NA,
            color = "#000000",
            lwd = 0.1) +

    xlab("") + ylab("") +
    
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
      axis.title.x = element_blank(),
      axis.title.y = element_blank(),
      axis.text = element_blank(),
      panel.grid = element_blank(),
      plot.margin = margin(0, 0, 0, 0, "cm")
    )
}


# map defaults

# defined at https://h3geo.org/docs/core-library/restable/
RES <- 3

column <- "es"
label <- "ES(50)"
trans <- "identity"
crs <- "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,50)

#extract coords to make grid
occ_sf_done <- lapply(occ_by_decade_filtered, function(x){
  do.call(rbind, st_geometry(x)) %>% 
  as_tibble() %>% setNames(c("decimallongitude","decimallatitude")) %>% 
  cbind(., x)
  })

# make grid

#by kingdom
kingdoms <- occ_sf_done[[1]]$kingdom %>% unique()

tictoc::tic()
grid_dec <- res_changes(data.table::rbindlist(occ_sf_done['1970'] %>% filter(kingdom = kingdoms[1])), RES)
#grid_dec <- res_changes(occ_sf_done[[1]], RES)
tictoc::toc()

#doing all decades at once took ~ 7 minutes to make res 2 grid

# make map
map <- gmap(grid_dec, column, label = label, trans = trans, crs = crs, limits = limits)

map

ggsave(
  paste0("output/tests/ES50_US_1970s_res_", RES, ".png"),
  plot = map,
  width = 1600,
  height = 800,
  units = "px",
  scale = 1,
  limitsize = FALSE
)


##Just CONUS

column <- "es"
label <- "ES(50)"
trans <- "identity"
crs <- "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,50)

gmap <- function(grid,
                 column = "shannon",
                 label = "Shannon index",
                 trans = "identity",
                 crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                 limits = c(0, 8)) {
  world <-
    rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
  bbglobe <- 'POLYGON ((-125 24, -65 24, -65 50, -125 50, -125 24))'
  
  sfc <- st_as_sfc(bbglobe, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
  bb <- sf::st_bbox(st_transform(sfc, crs))
  ggplot() +
    
    geom_sf(
      data = grid,
      aes_string(fill = column, geometry = "geometry"),
      show.legend = TRUE,
      lwd = 0
    ) +
    
    # viridis::scale_color_viridis(
    #   option = "inferno",
    #   na.value = "white",
    #   name = label,
    #   trans = trans,
    #   limits = limits
    # ) +
    
    viridis::scale_fill_viridis(
      option = "inferno",
      na.value = "white",
      name = label,
      trans = trans,
      limits = limits
    ) +
    
    geom_sf(data = world,
            fill = NA,
            color = "grey",
            lwd = 0.1) +
    
    geom_sf(data = USA_shape,
            fill = NA,
            color = "#000000",
            lwd = 0.1) +
    
    xlab("") + ylab("") +
    
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
      axis.title.x = element_blank(),
      axis.title.y = element_blank(),
      axis.text = element_blank(),
      panel.grid = element_blank(),
      plot.margin = margin(0, 0, 0, 0, "cm")
    )
}

map <- gmap(grid_dec, column, label = label, trans = trans, crs = crs, limits = limits)

map

ggsave(
  paste0("output/tests/ES50_CONUS_1970s_res_", RES, ".png"),
  plot = map,
  width = 1600,
  height = 800,
  units = "px",
  scale = 1,
  limitsize = FALSE
)

#CORRELATION between shannon and es50
ggplot(data = grid_dec, 
       aes(x = es,
           y = shannon)) +
  geom_point(size = 0.5,
             shape = 21,
             stroke = 0.1) +
  geom_smooth(method = "lm", lwd = 1) +
  labs(x = "ES50",
       y = "Shannon Diversity",
       title = "ES50 vs Shannon for GBIF US 1970s")

ggsave(filename = paste0("output/tests/ES50_v_shannon_USA_1970s_res_", RES, ".png"),
       width=1600,
       height=800,
       units="px",
       scale=1,
       limitsize = FALSE)

#CORRELATION between Hill 1 and es50
ggplot(data = grid_dec, 
       aes(x = es,
           y = hill_1)) +
  geom_point(size = 0.5,
             shape = 21,
             stroke = 0.1) +
  labs(x = "ES50",
       y = "Hill Number of 1",
       title = "ES50 vs Hill-1 for GBIF US 1970s")

ggsave(filename = paste0("output/tests/ES50_v_hill1_USA_1970s_res_", RES, ".png"),
       width=1600,
       height=800,
       units="px",
       scale=1,
       limitsize = FALSE)


# Compare ES50 to ES100 - one of analysis where I tweaked the above code

# es50_res2_1960_70s <- grid_dec$es
# 
# ES100_res2_1960_70s <- grid_dec$es
# 
# ggplot(data = data.frame(es50_res2_1960_70s, ES100_res2_1960_70s),
#        aes(x = es50_res2_1960_70s,
#            y = ES100_res2_1960_70s)) +
#   geom_point(size = 0.5,
#              shape = 21,
#              stroke = 0.1) +
#   labs(x = "ES50",
#        y = "ES100",
#        title = "ES50 vs ES100 for GBIF US 1970s") +
#   geom_smooth(method = "lm", lwd = 1, se = FALSE) +
#   geom_abline(slope = 1, intercept = 0)
# 
# ggsave(filename = paste0("output/tests/es50_v_ES100_USA_1970s_res_", RES, ".png"),
#        width=1600,
#        height=800,
#        units="px",
#        scale=1,
#        limitsize = FALSE)
