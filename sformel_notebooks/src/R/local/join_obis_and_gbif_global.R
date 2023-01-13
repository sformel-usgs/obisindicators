#Join obis and gbif

library(arrow)
library(dplyr)

#load data

#output filepath for voluminous data
data_path <- "C:/Users/sformel/Downloads/heavy_data_not_synced/global_biodiversity_map/"

obis <- open_dataset(sources = paste0(data_path,
                                      "processed/OBIS_2010s/"))
gbif <- open_dataset(sources = paste0(data_path,
                                      "processed/GBIF_2010s/"))
                     

#obis$schema
#gbif$schema



#Optimize working outside of arrow----

#Time working outside of arrow - about XX min

# tictoc::tic()
# obis_comp <- obis %>% 
#   select(#obisid, #it looks like this has a lot of NAs. I'm not sure why.
#     year,  #this should be eventdate, but they're a mess. So this is fine for now.
#     species,
#     cell,
#     hemisphere) %>% 
#   collect() #%>% 
# 
# tictoc::toc() # took 1 second
# 
# tictoc::tic()
# gbif_comp <- gbif %>% #it's about 4.3e8 records, could not be run. Need to break up into hemispheres
#   select(#gbifid,
#     year, 
#     species,
#     cell,
#     hemisphere) %>%
#   collect() #%>% 
#   #data.table::setDT() #This adds time, but I think we gain some of it back with the grouping and counting below
# tictoc::toc() #took 1.5 minutes
# 
# tictoc::tic()
# done <- list(obis_comp, gbif_comp)
# done <- data.table::rbindlist(done) #It turns out that rbind.data.frame is notoriously slow.  rbindlist is the way to go, about 1 min
# tictoc::toc() #took about XX min
# 
# tictoc::tic()
# done2 <- done[, #took about 2 min
#               .N,  #changge this name to "records"
#               by = .(year,
#                      cell,
#                      year,
#                      species)]
# 
# done2 <- done2 %>%  rename(records = N)
# tictoc::toc()
# 
# time_outside_arrow <- tictoc::toc()
# 
# #Calculate diversity indices
# source("src/R/obis_calc_indicators_improved_dt.R")
# 
# bio_idx <- done3 %>% obis_calc_indicators_improved_dt(esn = 100)
# 
# 
# h3_grids <- readRDS("data/processed/h3_grids/h3_res_1to5_globalgrid.rds")[[5]]
# 
# #join grid and indicators----
# grid <- inner_join(h3_grids,
#                    bio_idx,
#                    by = c("hex_ids" = "cell"))



# Split in to hemispheres ------


hemispheres <- c("NE", "NW", "SE", "SW") #need to investigate what to do with NA values from hemisphere

#x <- hemispheres[2] #for testing. Looks like NE + NW can't be handled on my computer, but either one individually works fine.

bio_idx <- lapply(hemispheres[c(2)], function(x) {
  
  tictoc::tic()
  
  obis_comp <- obis %>%
    select(#year, not needed save memory
           species,
           cell,
           hemisphere) %>%
    filter(hemisphere == x) %>%
    select(-hemisphere) %>%
    collect()
  
  gbif_comp <- gbif %>%
    select(#year, not needed, save memory
           species,
           cell,
           hemisphere) %>%
    filter(hemisphere == x) %>%
    select(-hemisphere) %>%
    collect()
  
  done <- list(obis_comp, gbif_comp)
  done <- data.table::rbindlist(done)
  
  done <- done[,
               .N,  #change this name to "records"
               by = .(cell,
                      #year,
                      species)]
  
  done <- done %>%  rename(records = N)
  
  #Calculate diversity indices
  source("src/R/obis_calc_indicators_improved_dt.R")
  
  bio_idx <-
    done %>% obis_calc_indicators_improved_dt(esn = 100) %>% 
    select(cell, es) #save memory
  
  tictoc::toc()
  
  return(bio_idx)
  
}) %>%
  data.table::rbindlist()

#For 2010s took about 


h3_grids <- readRDS(paste0(data_path,
                           "processed/h3_grids/h3_res_1to5_globalgrid.rds"))[[5]]

#join grid and indicators----
grid <- inner_join(h3_grids,
                   bio_idx,
                   by = c("hex_ids" = "cell"))



# map defaults
RES <- 5 # defined at https://h3geo.org/docs/core-library/restable/
column <- "es"
label <- paste0("ES", 100) #legend label
trans <- "identity"
crs <-
  "+proj=robin +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0, 100) #legend limits

# make map

#states
USA_shape <- rnaturalearth::ne_states(returnclass = "sf", country = "United States of America")
world <- rnaturalearth::ne_countries(scale = "small",
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
