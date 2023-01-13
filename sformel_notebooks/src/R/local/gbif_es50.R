#Create GBIF ES50 layer
#Based off original code from Abby Benson: use_gbif_parquet_file.rmd

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

#load libraries-----

library(obisindicators)
library(dplyr)
library(arrow)
library(sf)
library(ggplot2)

source("src/obis_calc_indicators_improved_dt.R")
source("src/gmap_discrete.R")

# get GBIF records

#changed path from europe to local US AWS server
parquet_path <- "s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/000000"
occ <- arrow::open_dataset(parquet_path)


tictoc::tic()

test <- occ %>%
    filter(countrycode == "BW",
    kingdom == "Fungi") %>%
  group_by(species) %>%
  count() %>%
  collect()

tictoc::toc()

# Test 
parquet_path <- "s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/000000"
occ <- arrow::open_dataset(parquet_path)

#get column structure and names
arrow::open_dataset(sources = parquet_path)$schema
arrow::open_dataset(sources = parquet_path)$schema$names

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local

occ_all <- occ %>%
  select(year, decimallongitude, decimallatitude, species) %>%
  filter(year == 1950) %>%
  group_by(decimallongitude,
           decimallatitude,
           species,
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# 61.89 sec elapsed when local, 9 seconds remote for one partition (i.e. 'row group')

#Remove year filter

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local

occ_all <- occ %>%
  select(year, decimallongitude, decimallatitude, species) %>%
  group_by(decimallongitude,
           decimallatitude,
           species,
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# 8.29 sec.  Wow. This is the way to go.

#First 10 partitions

parquet_path <- paste0("s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/00000", c(1:9))
occ <- arrow::open_dataset(parquet_path)

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local

occ_all <- occ %>%
  select(year, decimallongitude, decimallatitude, species) %>%
  group_by(decimallongitude,
           decimallatitude,
           species,
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# 20 sec.

#First 100 partitions

parquet_path <- c(paste0("s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/00000", c(1:9)),
                  paste0("s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/0000", c(10:99)))
occ <- arrow::open_dataset(parquet_path)

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local

occ_all <- occ %>%
  select(year, decimallongitude, decimallatitude, species) %>%
  group_by(decimallongitude,
           decimallatitude,
           species,
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

tictoc::toc()

# Memory broke down. So I should test this on Denali. Dask could handle the first 100 partitions without too much trouble on denali.


#Work on GFE-----
#Process and index to h3 for 10 partitions. Using local because it doesn't make sense to 
#process this from the cloud if you're intending on doing a glboal analysis.

#parquet_path <- paste0("s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/00000", c(1:9))
parquet_path <- paste0("data/GBIF_snapshot_2022-11-01/occurrence.parquet/00000", c(1:9))

#get column structure and names
arrow::open_dataset(sources = parquet_path)$schema
arrow::open_dataset(sources = parquet_path)$schema$names

tictoc::tic()

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local

occ_all <- occ %>%
  select(year, decimallongitude, decimallatitude, species) %>%
  filter(year != 1960) %>% 
  group_by(decimallongitude,
           decimallatitude,
           species,
           year) %>%  # remove duplicate rows
  filter(!is.na(species),
         !is.na(decimallatitude))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

occ_all$cells <- occ_all %>%
  select(decimallatitude,
         decimallongitude) %>%
  h3::geo_to_h3(latlng = .,
                res = 6)

tictoc::toc()

#Took:
# res 1 = 10.5 seconds
# res 2 = 11 seconds
# res 3 = 12 seconds
# res 4 = 12.6 seconds
# res 5 = 14 seconds
# res 6 = 15.75 seconds

# There are 9.8 million records. 

#How many times would I have to run this?
2.25e9 / 10e6 #225 loops

# At res 5:

225 * 14 /60 #1 hour to run all partitions.

#Next steps:  

#1. generate and store grids separately.
#2. Generate diversity indices by cell.


#Generate indexed occurrences ----

# Adding the write to parquet increased time to 14.5 seconds.Increasing to 20 partitions 
# increased time to 30 seconds and total records to 20e6.  So it seems more or less linear.

partitions <- list.files(path = "data/GBIF_snapshot_2022-11-01/occurrence.parquet/", full.names = TRUE)
#partitions <- split(partitions, f = ceiling(seq_along(partitions)/20))


#Apparently R arrow can't handle some logicals yet: https://issues.apache.org/jira/browse/ARROW-15014
#So the plan is to add the h3 index then write to csv, bring it all back in and finish the filtering and grouping there.

#This didn't work.  Apparently casting from list to string insn't implemented in R yet.
# chosen_schema <- schema(
#   gbifid = int64(),
#   datasetkey = string(),
#   occurrenceid = string(),
#   kingdom = string(),
#   phylum = string(),
#   class = string(),
#   order = string(),
#   family = string(),
#   genus = string(),
#   species = string(),
#   infraspecificepithet = string(),
#   taxonrank = string(),
#   scientificname = string(),
#   verbatimscientificname = string(),
#   verbatimscientificnameauthorship = string(),
#   countrycode = string(),
#   locality = string(),
#   stateprovince = string(),
#   occurrencestatus = string(),
#   individualcount = int32(),
#   publishingorgkey = string(),
#   decimallatitude = double(),
#   decimallongitude = double(),
#   coordinateuncertaintyinmeters = double(),
#   coordinateprecision = double(),
#   elevation = double(),
#   elevationaccuracy = double(),
#   depth = double(),
#   depthaccuracy = double(),
#   eventdate = timestamp(unit = "ms"),
#   day = int32(),
#   month = int32(),
#   year = int32(),
#   taxonkey = int32(),
#   specieskey = int32(),
#   basisofrecord = string(),
#   institutioncode = string(),
#   collectioncode = string(),
#   catalognumber = string(),
#   recordnumber = string(),
#   identifiedby = string(),
#   dateidentified = timestamp(unit = "ms"),
#   license = string(),
#   rightsholder = string(),
#   recordedby = string(),
#   typestatus = string(),
#   establishmentmeans = string(),
#   lastinterpreted = timestamp(unit = "ms"),
#   mediatype = string(),
#   issue = string()
# )

#This takes too long. New strategy is to just generate the h3 indices and 
# join it with the data later. After I commented out everything, it went from 80 to 2 seconds.

tictoc::tic()

lapply(partitions, function(x) {
  tictoc::tic()
  
  occ <- arrow::open_dataset(sources = x)
  
  #Somehow != isn't implemented yet in filter arrow
  occ_all <- occ %>%
    select(
      gbifid,
      #issue,
      #basisofrecord,
      #occurrencestatus,
      #kingdom,
      #class,
      #countrycode,
      #year,
      decimallongitude,
      decimallatitude,
      #species
    ) %>%
    collect()
  
  occ_all$cells <- occ_all %>%
    select(decimallatitude,
           decimallongitude) %>%
    h3::geo_to_h3(latlng = .,
                  res = 5)
  
  row_groups <- basename(x)

  #janky way to get issue from array to vector
  #occ_all$issue <- lapply(occ_all$issue, function(x){
  #  
  #  ifelse(length(x$array_element)==0,NA, x)
    
  #}) %>% unlist()
  
  write_parquet(x = occ_all %>% 
                  select(gbifid, cells),
                paste0(
                  "output/indexed_occ/h3idx_occurence_",
                  row_groups,
                  ".parquet"
                ))
  
  #While the csvs are ~ 5 times larget than the parquet, 
  #I won't encounter the limitations of unimplemented operators when I import it back in.
  
  # data.table::fwrite(occ_all %>% 
  #                      select(-decimallatitude, -decimallongitude),
  #                    file = paste0("output/indexed_occ/h3_5_idx_occurence",
  #                                  row_groups,
  #                                  ".csv"),
  #                    quote = FALSE,
  #                    na = NA)
tictoc::toc()
  
})


time_total <- tictoc::toc()

#Took 48 minutes to process


#Join the h3 indices with the parquet and calculate diversity index

partitions <- list.files(path = "data/GBIF_snapshot_2022-11-01/occurrence.parquet/", full.names = TRUE)
#partitionsh3 <- list.files(path = "output/indexed_occ", full.names = TRUE)

tictoc::tic()

Q <- lapply(partitions, function(x) {
  tictoc::tic()
  
  occ <- arrow::open_dataset(sources = x)
  
  #Somehow != isn't implemented yet in filter arrow
  occ_all <- occ %>%
    select(
      gbifid,
      #issue, couldn't figure out how to get it out of the array format efficiently
      basisofrecord,
      occurrencestatus,
      kingdom,
      class,
      countrycode,
      year,
      decimallongitude,
      decimallatitude,
      species
    ) %>%
    filter(occurrencestatus == "PRESENT") %>%  #adding this line actually reduced it by 4 seconds for partition 000000
    filter(basisofrecord == "OBSERVATION" |  #maybe reduced it slightly.
             basisofrecord == "MACHINE_OBSERVATION" | 
             basisofrecord == "HUMAN_OBSERVATION" |
             basisofrecord == "MATERIAL_SAMPLE" |
             basisofrecord == "PRESERVED_SPECIMEN" |
             basisofrecord == "OCCURRENCE") %>% 
    filter(!is.na(decimallatitude)) %>% #increased slightly from occurrencestatus filter
    select(gbifid, #increased it by 1 sec
           #issue,
           kingdom,
           class,
           countrycode,
           year,
           decimallongitude,
           decimallatitude,
           species) %>% 
    collect()
  
  #janky way to get issue from array to vector, but it doubles the time, so no good
  # occ_all$issue <- lapply(occ_all$issue, function(x){
  #   ifelse(length(x$array_element)==0,NA, x)
  #   }) %>% unlist()
  
#  occ_all$issue <- unlist(occ_all$issue, recursive = FALSE, use.names = FALSE) %>% 
#    vctrs::vec_cbind() #didn't add much time, if any

  # occ_all <- occ_all %>%
  #   filter(!(issue == "COUNTRY_COORDINATE_MISMATCH"),
  #          !(issue == "RECORDED_DATE_INVALID"),
  #          !(issue == "COORDINATE_INVALID"))
  # 
  
  row_groups <- basename(x)
  
  #This is faster than data.table
  arrow::write_parquet(x = occ_all,
                sink = paste0("output/indexed_occ/cleaned_occurrence/",
                              row_groups,
                              ".parquet"))
                    
  tictoc::toc()

  return(occ_all)
  
  })

tictoc::toc()


#With more clarity-----

tictoc::tic()

partitions <- list.files(path = "data/GBIF_snapshot_2022-11-01/occurrence.parquet/", full.names = TRUE)

#1. Open the parquet partition

future::plan(strategy = "multisession",
             workers = future::availableCores() * 0.75)

furrr::future_map(partitions, function(x){
#lapply(redo, function(x){

  tictoc::tic()

  occ <- arrow::open_dataset(sources = x)

#2. Reduce columns, filter where able and calculate the h3 index.
#At this point the coordinates can be discarded because the cell index is enough to calculate diversity.


occ_all <- occ %>%
  select(
    gbifid, #will serve as key for joins later on
    #issue, #couldn't figure out how to get it out of the array format efficiently
    basisofrecord,
    occurrencestatus,
    #kingdom, #will bring these in later via a join
    #class,
    decimallatitude,
    countrycode,
    year,
    species
  ) %>%
  filter(year >= 1960) %>%
  filter(countrycode == "US") %>% 
  filter(occurrencestatus == "PRESENT") %>%  #adding this line actually reduced it by 4 seconds for partition 000000
  filter(basisofrecord == "OBSERVATION" |  #maybe reduced it slightly.
           basisofrecord == "MACHINE_OBSERVATION" |
           basisofrecord == "HUMAN_OBSERVATION" |
           basisofrecord == "MATERIAL_SAMPLE" |
           basisofrecord == "PRESERVED_SPECIMEN" |
           basisofrecord == "OCCURRENCE") %>%
  filter(!is.na(species)) %>%
  filter(!is.na(decimallatitude)) %>%
  select(gbifid,
         #issue,
         #kingdom,
         #class,
         #countrycode,
         year,
         species) %>%
  collect()

#Calculate and add  cells

occ_all$cell <- occ %>%
  filter(year >= 1960) %>%
  filter(countrycode == "US") %>% 
  filter(occurrencestatus == "PRESENT") %>%  #adding this line actually reduced it by 4 seconds for partition 000000
  filter(basisofrecord == "OBSERVATION" |  #maybe reduced it slightly.
           basisofrecord == "MACHINE_OBSERVATION" |
           basisofrecord == "HUMAN_OBSERVATION" |
           basisofrecord == "MATERIAL_SAMPLE" |
           basisofrecord == "PRESERVED_SPECIMEN" |
           basisofrecord == "OCCURRENCE") %>%
  filter(!is.na(species)) %>%
  select(decimallatitude,
         decimallongitude) %>%
  filter(!is.na(decimallatitude)) %>%
  collect() %>%
  h3::geo_to_h3(latlng = .,
                res = 5)

#create records for diversity idx, this step takes the longest
# done <- occ_all %>%
#   select(year,
#          species,
#          cell) %>%
#   group_by(year,species,cell) %>%
#   summarise(records = n(),
#             .groups = "drop")
# 
#   rm(occ_all)

  row_groups <- basename(x)

#3. Write to parquet, partitioning by cell
  arrow::write_dataset(dataset = occ_all,
                     format = "parquet",
                     path = paste0("output/indexed_occ/cleaned_occurrence/",
                                   row_groups), partitioning = c("year"), 
                     existing_data_behavior = "overwrite")

  rm(done)

  tictoc::toc()

#})
}, .options = furrr::furrr_options(seed = NULL))

future::plan(strategy = "sequential")
time_total <-   tictoc::toc() #took a little over 3 hours (with record calc), but 4 didn't process for some reason.

#took minutes without bioidx and US only

# Q <- list.dirs(path = "output/indexed_occ/cleaned_occurrence/", recursive = FALSE)
# Q <- as.numeric(basename(Q))
# 
# W <- seq(1,2021,1)
# 
# D <- setdiff(W, Q)
# 
# redo <- partitions[as.numeric(basename(partitions)) %in% D]

#after re-running them, it became clear that they didn't have any dates >= 1960 and were dropped.

#Read them back in and calculating diversity and mapping.
#Ok, my GFE can't handle this for 2010s, it runs out of memory. I'll loop through years.

#at H3 = 5, this should end up being a 2e6 row data set.
# For the 2010s its only 1.65e6 because there aren't observations in every cell.
# Takes anywhere from 10 sec to 90 sec per year.

#calc diversity----

tictoc::tic()
  
  occ <- arrow::open_dataset(sources = "output/indexed_occ/")
  
  years <- seq(2010, 2019, 1)
  
  occ_all_idx <- lapply(years, function(x){
  

    tictoc::tic()  
    
    A <- occ %>%
      filter(year == x) %>%
      group_by(year,species,cell) %>%
      summarise(records = n(),
                .groups = "drop") %>% 
      select(-year) %>%
      collect()
    
    tictoc::toc()
    
    return(A)
    
  }) %>% data.table::rbindlist()%>%
    obis_calc_indicators_improved_dt(esn = 100)
  
time_diversity_cal <- tictoc::toc() #2010s took about 9 min, when it was just US it took about 2 min

#read in saved versions of the grids to save time.
#Worth noting that you can read the cell resolution by how many of the 16 characters are not "f", starting at position 4
h3_grids <- readRDS("output/h3_grids/h3_res_1to5_globalgrid.rds")[[5]]

#join grid and indicators----
grid <- inner_join(h3_grids,
                   occ_all_idx,
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

map <- gmap_discrete(
  grid,
  column,
  label = label,
  trans = trans,
  crs = crs,
  limits = limits
)

ggsave(filename = "output/tests/CONUS_2010s_res5.png",
  plot = map,
  width = 6400,
  height = 3200,
  units = "px",
  scale = 1,
  limitsize = FALSE)


#Address the issues-----


partitions <- list.files(path = "data/GBIF_snapshot_2022-11-01/occurrence.parquet/", full.names = TRUE)

tictoc::tic()

future::plan(strategy = "multisession",
             workers = future::availableCores() * 0.9)

I <- furrr::future_map(partitions, function(x){
#I <- lapply(partitions[c(20:50)], function(x){
  tictoc::tic()
 
#It might save a little time if you remove columns after you're done using them. Ugly code, but ow well.
  
  occ <- arrow::open_dataset(sources = x)
  
  occ_all <- occ %>%
    select(
      gbifid,
      issue,
      basisofrecord,
      occurrencestatus,
      countrycode,
      year
      ) %>%
    filter(year >= 1960) %>%
    select(-year) %>% 
    filter(countrycode == "US") %>%
    select(-countrycode) %>% 
    filter(occurrencestatus == "PRESENT") %>%  #adding this line actually reduced it by 4 seconds for partition 000000
    select(-occurrencestatus) %>% 
    filter(basisofrecord == "OBSERVATION" |  #maybe reduced it slightly.
             basisofrecord == "MACHINE_OBSERVATION" |
             basisofrecord == "HUMAN_OBSERVATION" |
             basisofrecord == "MATERIAL_SAMPLE" |
             basisofrecord == "PRESERVED_SPECIMEN" |
             basisofrecord == "OCCURRENCE") %>%
    select(gbifid,
           issue) %>% 
    collect()
  
  if(length(occ_all$issue) > 0){ #some partitions have no issues, and therefore are read as a null column
    
    print(x)
    issue <- data.table::rbindlist(occ_all$issue, idcol = TRUE) %>% 
    filter(array_element == "COUNTRY_COORDINATE_MISMATCH")
  
    gbifid <- occ_all$gbifid[issue$.id]
  
    done <- data.frame(gbifid, issue = issue$array_element)
    
    tictoc::toc()
    
    done
    
    } else {
      NULL
    }
    
    #print(x)
  
}, .options = furrr::furrr_options(seed = NULL)) %>% 
#}) %>% 
  data.table::rbindlist()

  #filter(lapply(issue, nrow) > 0) #another way to work with issues

future::plan(strategy = "sequential")
  
tictoc::toc()

