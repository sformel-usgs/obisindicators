##----Calc bio_idx----

#Join obis and gbif and make map

tictoc::tic()

setwd("/caldera/projects/css/sas/bio/spec_obs/global_biodiversity_map/")

library(arrow)
library(dplyr)
library(sf)
library(ggplot2)

source("src/R/HPC/gmap_discrete.R")
source("src/R/HPC/obis_calc_indicators_improved_dt.R")

#set ES sampling depth
es_depth <- 50

#set kingdoms to include
kings <- "Plantae"

#load data

obis <- open_dataset(sources = "data/processed/OBIS_2010s/")
gbif <- open_dataset(sources = "data/processed/GBIF_2010s/")

# Split in to hemispheres ------


hemispheres <- c("NE", "NW", "SE", "SW") #need to investigate what to do with NA values from hemisphere

#x <- hemispheres[2] #for testing. Looks like NE + NW can't be handled on my computer, but either one individually works fine.
# On Denali, NE hemisphere took about 4 min

tictoc::tic()
bio_idx <- lapply(hemispheres, function(x) {

  tictoc::tic()

  obis_comp <- obis %>%
    select(#year, not needed save memory
           species,
           cell,
           hemisphere,
	   kingdom) %>%
    filter(hemisphere == x) %>%
    select(-hemisphere) %>%
    filter(kingdom==kings) %>%
    collect()

  gbif_comp <- gbif %>%
    select(#year, not needed, save memory
           species,
           cell,
           hemisphere,
	   kingdom) %>%
    filter(hemisphere == x) %>%
    select(-hemisphere) %>%
    filter(kingdom==kings) %>%
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

  bio_idx <-
    done %>% obis_calc_indicators_improved_dt(esn = es_depth) %>%
    select(cell, es) #save memory

  tictoc::toc()

  return(bio_idx)

}) %>%
  data.table::rbindlist()

tictoc::toc()
#For 2010s took about 13 minutes. NE = 2.5 min, NW = 7 min, SE = 2 min, SW = 1.5 min. Table size == 533,546 x 2

##---- join with h3 grid----

#Made separately to save time and reuse
h3_grids <- readRDS("data/processed/h3_res_1to5_globalgrid.rds")[[5]]

#join grid and indicators----
tictoc::tic()
grid <- inner_join(h3_grids,
                   bio_idx,
                   by = c("hex_ids" = "cell"))
tictoc::toc() #took 16 seconds

##---- Make map ----

# map defaults
RES <- 5 # defined at https://h3geo.org/docs/core-library/restable/
column <- "es"
label <- paste0("ES", es_depth) #legend label
trans <- "identity"
crs <-
  "+proj=robin +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0, 100) #legend limits



#states
USA_shape <- rnaturalearth::ne_states(returnclass = "sf", country = "United States of America")
world <- rnaturalearth::ne_countries(scale = "small",
                              returnclass = "sf")

#Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them

#partly based on John Waller's plots: https://github.com/jhnwllr/es50
map <- gmap_discrete(
  grid,
  column,
  label = label,
  trans = trans,
  crs = crs,
  limits = limits
)

tictoc::tic()
ggsave(filename = paste0("output/tests/HPC/", label, "_global_2010s_res5_OBIS+GBIF_", kings, "_", Sys.Date(), ".png"),
        plot = map,
        width = 6400,
        height = 3200,
        units = "px",
        scale = 1,
        limitsize = FALSE)

tictoc::toc() #took 2.5 min

total_runtime <- tictoc::toc()
