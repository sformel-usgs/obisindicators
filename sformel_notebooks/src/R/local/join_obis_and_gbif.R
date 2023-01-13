#Join obis and gbif

library(arrow)
library(dplyr)

#load data

#output filepath for voluminous data
data_path <- "C:/Users/sformel/Downloads/heavy_data_not_synced/global_biodiversity_map/"

obis <- open_dataset(sources = paste0(data_path,
                                      "processed/OBIS_USA_2010s/"))
gbif <- open_dataset(sources = paste0(data_path,
                                      "processed/GBIF_USA_2010s/"))
                     

obis$schema
gbif$schema


#Time working outside of arrow - about 13 min
tictoc::tic()

# obis_comp <- obis %>% 
#   select(#obisid, #it looks like this has a lot of NAs. I'm not sure why.
#          year,  #this should be eventdate, but they're a mess. So this is fine for now.
#          decimallatitude,
#          decimallongitude,
#          species) %>% 
#   collect() %>% 
#   data.table::setDT()
#   
# 
# gbif_comp <- gbif %>% #it's about 4.3e8 records
#   select(#gbifid,
#          year, 
#          decimallatitude,
#          decimallongitude,
#          species) %>%
#   collect() %>% 
#   data.table::setDT() #This adds time, but I think we gain someof it back with the grouping and counting below
# 
# done <- rbind.data.frame(obis_comp, gbif_comp) #This takes longer than I thought it would
# 
# 
# done2 <- done[,
#      .N,  #chaneg this name to "records"
#      by = .(year,
#             decimallatitude,
#             decimallongitude,
#             year,
#             species)]
# 
# time_outside_arrow <- tictoc::toc()

#Time working inside of arrow - ran about 10 min and then crashed.  So even if it was fster, it's not by much, and requires too much memory to run on GFE. 
#I'm assuming it's the groupby and summarize calls that are very memory intensive.

# done <- inner_join(
#   x = (obis %>% 
#   select(#obisid, #it looks like this has a lot of NAs. I'm not sure why.
#     year,  #this should be eventdate, but they're a mess. So this is fine for now.
#     decimallatitude,
#     decimallongitude,
#     species)), 
#   y = (gbif %>% #it's about 4.3e8 records
#   select(#gbifid,
#     year, 
#     decimallatitude,
#     decimallongitude,
#     species))) %>% 
#     group_by(year,
#              decimallatitude,
#              decimallongitude,
#              species) %>%
#     summarise(records = n()) %>%   
#   collect() 
# 
# 
# 
# time_inside_arrow <- tictoc::toc()

#Optimize working outside of arrow----

#Time working outside of arrow - about XX min

tictoc::tic()
obis_comp <- obis %>% 
  select(#obisid, #it looks like this has a lot of NAs. I'm not sure why.
    year,  #this should be eventdate, but they're a mess. So this is fine for now.
    decimallatitude,
    decimallongitude,
    species) %>% 
  collect() #%>% 

tictoc::toc() # took 1 second

tictoc::tic()
obis_comp$cell <- obis_comp %>% 
  select(decimallatitude,
       decimallongitude) %>%
  h3::geo_to_h3(latlng = .,
                res = 5)
tictoc::toc() #took 0.5 seconds

tictoc::tic()
obis_comp$hemisphere <- case_when(obis_comp$cell %in% NW_hex_ids ~ "NW")
tictoc::toc() #took 0.5 seconds

tictoc::tic()
gbif_comp <- gbif %>% #it's about 4.3e8 records
  select(#gbifid,
    year, 
    decimallatitude,
    decimallongitude,
    species) %>%
  collect() #%>% 
  #data.table::setDT() #This adds time, but I think we gain someof it back with the grouping and counting below
tictoc::toc() #took 1.5 minutes

tictoc::tic()
done <- list(obis_comp, gbif_comp)
done <- data.table::rbindlist(done) #It turns out that rbind.data.frame is notoriously slow.  rbindlist is the way to go, about 1 min
tictoc::toc() #took about 4 min

tictoc::tic()
done2 <- done[, #took about 2 min
              .N,  #changge this name to "records"
              by = .(year,
                     decimallatitude,
                     decimallongitude,
                     year,
                     species)]

done2 <- done2 %>%  rename(records = N)
tictoc::toc()

time_outside_arrow <- tictoc::toc()


#index to h3----

done2$cell <- done2 %>%
#done2$cell <- obis_comp %>%
  select(decimallatitude,
         decimallongitude) %>%
  h3::geo_to_h3(latlng = .,
                res = 5)

#subset to NW hemisphere
done3 <- done2 %>% 
  filter(cell %in% NW_hex_ids)

source("src/R/obis_calc_indicators_improved_dt.R")

bio_idx <- done3 %>% obis_calc_indicators_improved_dt(esn = 100)


h3_grids <- readRDS("data/processed/h3_grids/h3_res_1to5_globalgrid.rds")[[5]]

#join grid and indicators----
grid <- inner_join(Q,
                   bio_idx,
                   by = c("hex_ids" = "cell"))

# map defaults
RES <- 5 # defined at https://h3geo.org/docs/core-library/restable/
column <- "es"
label <- paste0("ES", 100) #legend label
trans <- "identity"
crs <-
  "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0, 100) #legend limits

# make map

#states
USA_shape <-
  rnaturalearth::ne_states(returnclass = "sf", country = "United States of America")
world <-
  rnaturalearth::ne_countries(country = "United States of America",
                              scale = "small",
                              returnclass = "sf")

#Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them

#partly based on John Waller's plots: https://github.com/jhnwllr/es50
source("src/R/gmap_discrete.R")
library(sf)
library(ggplot2)

map <- gmap_discrete(
  grid,
  column,
  label = label,
  trans = trans,
  crs = crs,
  limits = limits
)

map


# ggsave(filename = "output/tests/ES50_CONUS_2010s_res5_OBIS+GBIF.png",
#        plot = map,
#        width = 6400,
#        height = 3200,
#        units = "px",
#        scale = 1,
#        limitsize = FALSE)
