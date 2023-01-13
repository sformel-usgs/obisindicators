#ES50 calculated for United STates on USGS HPC Denali
#Based off original code from Abby Benson: use_gbif_parquet_file.rmd

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

library(obisindicators)
library(dplyr)
library(sf)
library(arrow)
library(magick)
library(ggplot2)
library(readr)


#Create list of queries by decade
year_list <- seq(from = 2019, to = 2022, by = 1)

#Function from Matt Biddle for h3 instead of dggridr

## Create function to make grid, calculate metrics for different resolution grid sizes

res_changes <- function(occ, resolution = 9){
  
  hex <- obisindicators::make_hex_res(resolution)
  
  occ <- occ %>%
    mutate(
      cell = h3::geo_to_h3(
        data.frame(decimallatitude,decimallongitude), #changed capitalization
        res = resolution
      )
    )
  
  idx <- obisindicators::calc_indicators(occ)
  
  grid <- hex %>%
    inner_join(
      idx,
      by = c("hexid" = "cell")
    )
}  


#Crunch Data

parquet_path <- "/caldera/projects/css/sas/bio/spec_obs/gbif/snapshot/occurrence.parquet/"

occ <- arrow::open_dataset(sources = parquet_path)

tictoc::tic()

lapply(year_list, function(x) {

  tictoc::tic()

  occ %>%
    select(countrycode, year, decimallongitude, decimallatitude, kingdom, class, species) %>%
    filter(countrycode=="US",
           year == x) %>%
    group_by(decimallongitude,
             decimallatitude,
             kingdom,
             class,
             species,
             year) %>%
    filter(!is.na(species))  %>%
    summarize(records = n(),
              .groups = "drop") %>%
    collect() %>%
    write.csv(x = ., paste0("/caldera/projects/css/sas/bio/spec_obs/gbif/output/gbif_USA_", x, ".csv"), quote = FALSE, row.names = FALSE)

  print(x)
  tictoc::toc()
})

print('Total Time')
tictoc::toc()

parquet_path <- "/caldera/projects/css/sas/bio/spec_obs/gbif/snapshot/occurrence.parquet/"

occ <- arrow::open_dataset(sources = parquet_path)

tictoc::tic()

lapply(year_list, function(x) {
  
  tictoc::tic()
  
  occ %>%
    select(countrycode, year, decimallongitude, decimallatitude, kingdom, class, species) %>%
    filter(year == x) %>%
    group_by(decimallongitude,
             decimallatitude,
             kingdom,
             class,
             species,
             year) %>%
    filter(!is.na(species))  %>%
    summarize(records = n(),
              .groups = "drop") %>%
    collect() %>%
    write.csv(x = ., paste0("/caldera/projects/css/sas/bio/spec_obs/gbif/output/gbif_", x, ".csv"), quote = FALSE, row.names = FALSE)
  
  print(x)
  tictoc::toc()
})

print('Total Time')
tictoc::toc()

#I double checked, a single node with 192 GB of RAM could not handle all the years at once.


#Read in csvs as data frame

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_196\\d.csv") #where you have your files
occ_1960 <- do.call(rbind,lapply(file_names,read_csv))

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_197\\d.csv") #where you have your files
occ_1970 <- do.call(rbind,lapply(file_names,read_csv))

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_198\\d.csv") #where you have your files
occ_1980 <- do.call(rbind,lapply(file_names,read_csv))

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_199\\d.csv") #where you have your files
occ_1990 <- do.call(rbind,lapply(file_names,read_csv))

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_200\\d.csv") #where you have your files
occ_2000 <- do.call(rbind,lapply(file_names,read_csv))

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_201\\d.csv") #where you have your files
occ_2010 <- do.call(rbind,lapply(file_names,read_csv))

file_names <- list.files(path = "output/gbif_usa_1969_2022/", full.names = TRUE, pattern = "^gbif_USA_202\\d.csv") #where you have your files
occ_2020 <- do.call(rbind,lapply(file_names,read_csv))



## Create a function for plotting from Matt Biddle

gmap <- function(
    grid, column = "shannon", label = "Shannon index", trans = "identity",
    crs="+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
    limits = c(0,8)) {
  
  world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf") %>% 
    filter(sovereignt == "United States of America")
  #world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
  #bbglobe <- 'POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))'
  bbglobe <- 'POLYGON ((-130 50, -130 23, -64 23, -64 50, -130 50))' #USA
  sfc <- st_as_sfc(bbglobe,crs='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
  bb <- sf::st_bbox(st_transform(sfc, crs))
  
  ggplot() +
    
    geom_sf(
      data = grid,
      aes_string(fill = column, geometry = "geometry"),
      show.legend = TRUE,
      lwd = 0
    ) +
    theme(legend.position = "right") +
    
    # viridis::scale_color_viridis(
    #   option = "inferno",
    #   na.value = "white",
    #   name = label,
    #   trans = trans,
    #   limits = limits
    # ) +

    #Try direction for reversing colors
    viridis::scale_fill_viridis(  
      option = "inferno",
      na.value = "white",
      name = label,
      trans = trans,
      limits = limits,
      direction = 1
    ) +

    geom_sf(data = world, fill = NA, color = "#000000") +

    xlab("") + ylab("") +

    coord_sf(crs  = crs,
             xlim = bb[c("xmin", "xmax")],
             ylim = bb[c("ymin", "ymax")]) +
    theme_classic()
  # theme(
    #   panel.background = element_rect(fill = "grey",
    #                                   colour = "grey",
    #                                   size = 0, linetype = "blank"),
    #   axis.ticks = element_blank(),
    #   axis.title.x = element_blank(),
    #   axis.title.y = element_blank(),
    #   axis.text = element_blank(),
    #   panel.grid = element_blank(),
    #   plot.margin = margin(0,0,0,0, "cm"))
}

# Lets do some work!


# grid resolution
# defined at https://h3geo.org/docs/core-library/restable/
RES <- 3

# map defaults
column <- "es"
label <- "ES(50)"
trans <- "identity"
crs <- "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,50)

# make grid for each level of a factor (kingdom, decade, etc)

grid_2 <- occ_1960 %>% 
  filter(!is.na(decimallatitude)) %>% 
  res_changes(occ = , resolution = 2)

tictoc::tic()

grid_dec <- occ_1960 %>% 
  filter(!is.na(decimallatitude)) %>% 
  res_changes(occ = , resolution = RES)

tictoc::toc()


# make map

bbglobe <- 'POLYGON ((-130 50, -130 23, -64 23, -64 50, -130 50))' #USA
sfc <- st_as_sfc(bbglobe,crs='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
bb <- sf::st_bbox(st_transform(sfc, crs))


map <-
  gmap(
    grid_2,
    column,
    label = label,
    trans = trans,
    crs = crs,
    limits = limits
  ) +
  geom_sf(
    data = grid_dec,
    aes_string(fill = column, geometry = "geometry"),
    show.legend = FALSE,
    lwd = 0) +
  viridis::scale_fill_viridis(  
    option = "inferno",
    na.value = NA,
    name = label,
    trans = trans,
    limits = limits
  ) +
  
  geom_sf(data = world, fill = NA, color = "#000000") +
  
  xlab("") + ylab("") +
  
  coord_sf(crs  = crs,
           xlim = bb[c("xmin", "xmax")],
           ylim = bb[c("ymin", "ymax")])
map

filename <- "output/nested_cells_gbif_1960-1969.jpg"

ggsave(filename = filename,
       plot=map,
       width=16000,
       height=8000,
       units="px",
       scale=1,
       limitsize = FALSE)

map <-
  gmap(
    grid_dec,
    column,
    label = label,
    trans = trans,
    crs = crs,
    limits = limits
  )
map

filename <- "output/gbif_ES50_h4_res3_1960-1969.jpg"
                    
ggsave(filename = filename,
       plot=map,
       width=16000,
       height=8000,
       units="px",
       scale=1,
       limitsize = FALSE)

#CORRELATION between shannon and es50
ggplot(data = grid_dec, 
  aes(x = es,
      y = shannon)) +
  geom_point() +
  geom_smooth(method = "lm")

ggsave(filename = "output/es50_v_shannon_USA_1960s.png",
       width=16000,
       height=8000,
       units="px",
       scale=1,
       limitsize = FALSE)

#By Kindom
library(future)
library(furrr)

tictoc::tic()

plan(strategy = multisession, workers = availableCores()*0.75)

grid_dec <-
  future_map(levels(as.factor(occ_1960$kingdom)), function(x) {
    occ_1960 %>%
      filter(!is.na(decimallatitude)) %>%
      filter(kingdom == x) %>% 
      res_changes(resolution = 3)
  })

tictoc::toc()

names(grid_dec) <- levels(as.factor(occ_1960$kingdom))
#plot and save

for (i in 1:length(grid_dec)) {
  map <- gmap(
    grid_dec[[i]],
    column,
    label = label,
    trans = trans,
    crs = crs,
    limits = limits
  )
  
  filename <-
    paste0("output/gbif_ES50_h4_res3_",
           names(grid_dec)[[i]],
           ".jpg")
  
  ggsave(
    filename = filename,
    device = "jpeg",
    plot = map,
    width = 16000,
    height = 8000,
    units = "px",
    scale = 1,
    limitsize = FALSE
  )
  
}

plot(grid_dec$es, grid_dec$hill_1)
plot(exp(grid_dec$es), grid_dec$hill_1)

plot(grid_dec$es, grid_dec$hill_2)
plot(grid_dec$es, grid_dec$shannon)

plot(grid_dec$es, grid_dec$sp)
plot(grid_dec$es, grid_dec$simpson)
plot(grid_dec$shannon, grid_dec$hill_1)

## Create maps by decade (this takes some time)

# Filer by decade
decs <- seq(1960, 2020, by=10)
# grid resolution
RES <- 3
# map defaults
column <- "es"
label <- "ES(50)"
trans <- "identity"
crs <- "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,50)

for (dec in decs){
  
  dec_beg = dec
  dec_end = dec_beg+9
  
  occ_dec <- occ %>%
    filter(
      date_year >= dec_beg,
      date_year <= dec_end)
  
  # make grid
  grid_dec <- res_changes(occ_dec, RES)
  
  # make map
  map <- gmap(grid_dec, column, label = label, trans = trans, crs = crs, limits = limits)
  # save map
  filename <- sprintf("images/globe/h3_map_obis_%s.jpg",dec_beg)
  ggsave(filename = filename,
         plot=map,
         width=16000,
         height=8000,
         units="px",
         scale=1,
         limitsize = FALSE)
  ime <- image_read(filename)
  # 
  # crop map
  ime <- image_crop(ime,"14546x7294+727+348")
  # ime <- image_trim(ime,fuzz=10)
  # 
  image_write(ime, path = filename, format = "jpg")
  
  
  ## Create all years
  
  # grid resolution
  RES <- 3
  # map defaults
  column <- "es"
  label <- "ES(50)"
  trans <- "identity"
  crs <- "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
  limits <- c(0,50)
  # make grid
  grid_dec <- res_changes(occ, RES)
  # make map
  map <- gmap(grid_dec, column, label = label, trans = trans, crs = crs, limits = limits)
  # save map
  filename <- "images/globe/h3_map_obis_all.jpg"
  ggsave(filename = filename,
         plot=map,
         width=16000,
         height=8000,
         units="px",
         scale=1,
         limitsize = FALSE)
  ime <- image_read(filename)
  # 
  # crop map
  ime <- image_crop(ime,"14546x7294+727+348")
  # ime <- image_trim(ime,fuzz=10)
  # 
  image_write(ime, path = filename, format = "jpg")
  
  ## Create and save colorbar
  # ggpubr does this for you
  library(ggpubr)
  df <- data.frame(x = seq(limits[1],limits[2],by=1) , y = seq(limits[1],limits[2], by=1))
  p <- ggplot(data = df, aes(x = x, y = y, colour = y)) + 
    geom_point() +
    viridis::scale_color_viridis(option = "inferno", name = "ES50") +
    theme_minimal() + 
    theme(legend.title = element_text(color='white',size = 10), 
          legend.text = element_text(color='white',size = 10))
  leg <- get_legend(p)
  legend <- as_ggplot(leg)
  filename = "images/globe/h3_legend.jpg"
  ggsave(filename = filename,
         plot=legend,
         units="px",
         scale=0.5,
         bg="transparent"
  )
  ime <- image_read(filename)
  # 
  # crop map
  #ime <- image_crop(ime,"14546x7294+727+348")
  ime <- image_trim(ime,fuzz=1)
  # 
  image_write(ime, path = filename, format = "jpg")
  
  ## create animated gif
  # img <- c(png_map_1960s, png_map_1970s, png_map_1980s, png_map_1990s, png_map_2000s, png_map_2010s, png_map_2020s)
  # 
  # image_append(image_scale(img, "x200"))
  # 
  # obis_es50_gif <- image_animate(image_scale(img, "1200x1200"), fps = 1, dispose = "previous")
  # image_write(obis_es50_gif, "images/obis_es50.gif")
  