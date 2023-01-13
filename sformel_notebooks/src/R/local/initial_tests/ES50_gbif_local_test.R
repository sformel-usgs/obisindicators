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
library(dggridR)


#####################
#Local vs Cloud call#
#####################

#Cloud

# tictoc::tic()
# parquet_path <-  "s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet"
# 
# #get column structure and names
# arrow::open_dataset(sources = parquet_path)$schema
# arrow::open_dataset(sources = parquet_path)$schema$names
# 
# occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local
# #occ <- arrow::read_parquet(file = parquet_path, col_select = c("year")) #doesn't work, get errors online and local
# 
# # In OBIS it's date_year and it's calculated for all records I believe, I'm not
# # sure if the same is true for year in GBIF- how do we check?
# 
# occ_all <- occ %>%	
#   select(order, countrycode, year, decimallongitude, decimallatitude, species) %>% 
#   filter(countrycode == "US", 
#          year == 1950,
#          !is.na(decimallongitude)) %>% 
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
# 
# # 3134.31 sec elapsed

#Local

#tictoc::tic()

parquet_path <- "../../../Documents/GBIF/snapshot/occurrence.parquet/"

#get column structure and names
arrow::open_dataset(sources = parquet_path)$schema
arrow::open_dataset(sources = parquet_path)$schema$names

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local

# In OBIS it's date_year and it's calculated for all records I believe, I'm not
# sure if the same is true for year in GBIF- how do we check?

# occ_all <- occ %>%	
#   select(year, decimallongitude, decimallatitude, species) %>% 
#   filter(year == 1950) %>% 
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

# 61.89 sec elapsed


#How many years are there?
# occ %>% 
#   select(year) %>% 
#   group_by(year) %>% 
#   count() %>% 
#   arrange(desc(year)) %>% 
#   collect() %>% 
#   dim()


#Parallel loop through years

#Run in parallel - doesnt' work with future/furrr because the arrow function is pointing, not an object

# #monitor time
# plan(strategy = multisession, workers = availableCores()*0.75)
# 
# A <- future_map(years_table$year[c(1:5)],
#                 function(x){
#                   occ %>%
#                     select(order,
#                            countrycode,
#                            year,
#                            decimallongitude,
#                            decimallatitude,
#                            species) %>%
#                     filter(countrycode == "US",
#                            year == x,!is.na(decimallongitude)) %>%
#                     group_by(decimallongitude,
#                              decimallatitude,
#                              species,
#                              year) %>%  # remove duplicate rows
#                     filter(!is.na(species))  %>%
#                     summarize(records = n(),
#                               .groups = "drop") %>%
#                     collect()}, 
#                 .options = furrr_options(seed = NULL)) 
# 
# #%>%   data.table::rbindlist(., fill = TRUE, idcol = 'origin')

#revert to serial
#plan('sequential')


#Tried with foreach, but it's the same problem

#How long does it take to run all the US?
tictoc::tic()

parquet_path <- "../../../Documents/GBIF/snapshot/occurrence.parquet/"

#Create list of queries by decade
decades <- seq(from = 1960, to = 2000, by = 1)

occ <- arrow::open_dataset(sources = parquet_path)

dim(occ)

occ_60 <- lapply(decades, function(x) {
  occ %>%	
    select(class, countrycode, year, decimallongitude, decimallatitude, species) %>% 
    filter(class!="Aves",
           countrycode=="US",
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
}) %>% 
  data.table::rbindlist(., fill = TRUE)

tictoc::toc()


occ_60 <- lapply(decades, function(x) {
  occ %>%	
    select(countrycode, year, decimallongitude, decimallatitude, species) %>% 
    filter(countrycode=="US",
           year == x) %>% 
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





#Create list of queries by decade
decades <- seq(from = 1960, to = 2020, by = 10) %>% 
  as.list()

occ_by_decade <- lapply(decades, function(x) {
  occ_all %>%
    filter(year >= x,
           year <= x + 9)
})

## Below is from ddgridr. Should we translate to h3?
# Create function to make grid, calculate metrics for different resolution grid sizes

res_changes <- function(occur, resolution = 9) {
  dggs <-
    dgconstruct(projection = "ISEA",
                topology = "HEXAGON",
                res = resolution)
  occur$cell <-
    dgGEO_to_SEQNUM(dggs, occur$decimallongitude, occur$decimallatitude)[["seqnum"]]
  idx <- calc_indicators(occur)
  
  grid <- dgcellstogrid(dggs, idx$cell) %>%
    st_wrap_dateline() %>%
    rename(cell = seqnum) %>%
    left_join(idx,
              by = "cell")
  return(grid)
}

## plot stuff
grid_list <- lapply(occ_by_decade, res_changes, resolution = 6)

names(grid_list) <- as.character(c(decades))

#There are some explicit arguments in hear that are needed for the HPC but aren't needed on most personal computers.

lapply(names(grid_list), function(x) {
  gmap_indicator(grid_list[[x]], "es", label = "ES(50)") %>%
    ggsave(plot = .,
           filename = paste0("/home/sformel/ES50_gbif/images/map_gbif_es_USA_", x, ".png"),
           width = 4,
           height = 4,
           units = "in",
           scale = 1,
           device = "png")
})

## create animated gif
decade_images <- list.files(path = "ES50_gbif/images", pattern = "*.png", full.names = TRUE)
img <- lapply(decade_images, image_read) %>% 
  image_join()

image_append(image_scale(img, "x200"))

gbif_es50_gif <- image_animate(image_scale(img, "1200x1200"), fps = 1, dispose = "previous") 
image_write(gbif_es50_gif, "/home/sformel/ES50_gbif/images/gbif_es50_animated_USA.gif")
