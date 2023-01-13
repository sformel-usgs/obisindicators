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
#library(dggridR)


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

#Create list of queries by decade
decades <- seq(from = 1960, to = 1999, by = 10)


# tictoc::tic()
# 
# parquet_path <- "/caldera/projects/css/sas/bio/spec_obs/gbif/snapshot/occurrence.parquet/"
# 
# #get column structure and names
# arrow::open_dataset(sources = parquet_path)$schema
# arrow::open_dataset(sources = parquet_path)$schema$names
# 
# occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local
# 
# occ_all <- occ %>%	
#   select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
#   filter(countrycode=="US",
#          year == 1950) %>% 
#   group_by(decimallongitude, 
#            decimallatitude, 
#            species, 
#            year) %>%  # remove duplicate rows
#   filter(!is.na(species))  %>%
#   summarize(records = n(),
#             .groups = "drop") %>%
#   collect()
# 
# tictoc::toc()

#Took 41 secs, about 2/3 the time of my GFE

#Can it handle all the world? Yes if you loop through it by decade.


tictoc::tic()

parquet_path <- "/caldera/projects/css/sas/bio/spec_obs/gbif/snapshot/occurrence.parquet/"

occ <- arrow::open_dataset(sources = parquet_path)

occ_all <- lapply(decades, function(x) {
  occ %>%	
  select(year, decimallongitude, decimallatitude, species) %>% 
  filter(year >= x,
         year <= x+9) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()
}) %>% 
  data.table::rbindlist(., fill = TRUE)

tictoc::toc()


#1960,70, and 80 ran in 167 seconds
#1990 ran in 92 seconds
#2000s would not complete in a reasonable time
#2000 - 2001 ran in 105 seconds

#1960 - 1999 ran in 383 seconds on Tallgrass


#Run on Tallgrass, which I believe shares memory across users, so I'm not sure how much was actually available.

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

#1960s
occ_60 <- occ %>%	
    select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
    filter(countrycode=="US",
           year >= 1960,
           year <= 1969) %>% 
    group_by(decimallongitude, 
             decimallatitude, 
             species, 
             year) %>%  # remove duplicate rows
    filter(!is.na(species))  %>%
    summarize(records = n(),
              .groups = "drop") %>%
    collect()

tictoc::toc()

# Took 94 seconds

#1970s

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

occ_70 <- occ %>%	
  select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
  filter(countrycode=="US",
         year >= 1970,
         year <= 1979) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Took 45 seconds

#1980s

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

occ_80 <- occ %>%	
  select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
  filter(countrycode=="US",
         year >= 1980,
         year <= 1989) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Took 48 seconds

#1990s

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

occ_90 <- occ %>%	
  select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
  filter(countrycode=="US",
         year >= 1990,
         year <= 1999) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Took 51 seconds

#2000s

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

occ_2010 <- occ %>%	
  select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
  filter(countrycode=="US",
         year >= 2000,
         year <= 2009) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Took 55 seconds

#2010s

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

occ_2010 <- occ %>%	
  select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
  filter(countrycode=="US",
         year >= 2010,
         year <= 2019) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Took XX seconds

#2020s

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path)

occ_2020 <- occ %>%	
  select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
  filter(countrycode=="US",
         year >= 2020,
         year <= 2029) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Took XX seconds


#RDS created on tallgrass

occ_all <- readr::read_rds("~/../../Downloads/GBIF_US_1060-1999.rds")

occ_all <- occ_all %>% 
  filter(!is.na(decimallongitude))

occ_by_decade <- lapply(decades, function(x) {
  occ_all %>%
    filter(year >= x,
           year <= x + 9)
})


## Create a function for plotting from Matt Biddle

gmap <- function(
    grid, column = "shannon", label = "Shannon index", trans = "identity",
    crs="+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
    limits = c(0,8)) {
  
  world <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
  bbglobe <- 'POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))'
  sfc <- st_as_sfc(bbglobe,crs='+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
  bb <- sf::st_bbox(st_transform(sfc, crs))
  ggplot() +
    
    geom_sf(
      data = grid, aes_string(
        fill = column, geometry = "geometry"), show.legend = FALSE, lwd=0) +
    
    viridis::scale_color_viridis(
      option = "inferno", na.value = "white",
      name = label, trans = trans, limits = limits) +
    
    viridis::scale_fill_viridis(
      option = "inferno", na.value = "white",
      name = label, trans = trans, limits = limits) +
    
    geom_sf(
      data = world, fill = NA, color = "#000000") +
    
    xlab("") + ylab("") +
    
    coord_sf(
      crs  = crs,
      xlim = bb[c("xmin","xmax")],
      ylim = bb[c("ymin","ymax")]) +
    
    theme(
      panel.background = element_rect(fill = "grey",
                                      colour = "grey",
                                      size = 0, linetype = "blank"),
      axis.ticks = element_blank(),
      axis.title.x = element_blank(),
      axis.title.y = element_blank(),
      axis.text = element_blank(),
      panel.grid = element_blank(),
      plot.margin = margin(0,0,0,0, "cm"))
}

# Lets do some work!


# grid resolution
# defined at https://h3geo.org/docs/core-library/restable/
RES <- 3

# map defaults
column <- "es"
label <- "ES(50)"
trans <- "identity"
crs <- "+proj=eqc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,50)
occ_dec <- occ_all %>%
  filter(
    year >= 1960,
    year <= 1999)

# make grid
grid_dec <- res_changes(occ_dec, RES)

#proc.time() - ptm

# make map
map <- gmap(grid_dec, column, label = label, trans = trans, crs = crs, limits = limits)

map

ggsave("output/ES50_gbif_1960-1999.pdf", width = 18, height = 8.5, units = "in")

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
