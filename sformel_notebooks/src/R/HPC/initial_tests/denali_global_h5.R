# Process GBIF parquet on a Denali node and index it to h5, output parquet

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

# Load libraries-------

library(obisindicators)
library(dplyr)
library(arrow)

#Load data -------

#tictoc::tic()

# version 2022-11-01

parquet_path <- "/caldera/projects/css/sas/bio/spec_obs/gbif/snapshot/occurrence.parquet"

#get column structure and names
arrow::open_dataset(sources = parquet_path)$schema
arrow::open_dataset(sources = parquet_path)$schema$names

occ <- arrow::open_dataset(sources = parquet_path) #works when calling online or local


#It looks like the %in% operator isn't implemented in arrow, so you have to get more basic for filtering.

occ_all <- occ %>%
  select(gbifid,
         issue,
         basisofrecord,
         occurrencestatus,
         kingdom,
         class,
         countrycode,
         year, 
         decimallongitude, 
         decimallatitude, 
         species) %>%
  filter(year == 1960) %>% 
  filter(issue != "['COUNTRY_COORDINATE_MISMATCH']") %>% 
  filter(issue != "['RECORDED_DATE_INVALID']") %>% 
  filter(issue != "['COORDINATE_INVALID']") %>%
  filter(occurrencestatus == "PRESENT") %>%
  filter(basisofrecord == "OBSERVATION") %>% 
  filter(basisofrecord == "MACHINE_OBSERVATION") %>% 
  filter(basisofrecord == "HUMAN_OBSERVATION") %>% 
  filter(basisofrecord == "MATERIAL_SAMPLE") %>% 
  filter(basisofrecord == "PRESERVED_SPECIMEN") %>% 
  filter(basisofrecord == "OCCURRENCE") %>% 
  filter(!is.na(species),
         !is.na(decimallatitude))  %>%
  group_by(gbifid,
           kingdom,
           class,
           decimallongitude,
           decimallatitude,
           species,
           year) %>%
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


# Generate indexed occurrences
# Adding the write to parquet increased time to 14.5 seconds.Increasing to 20 partitions 
# increased time to 30 seconds and total records to 20e6.  So it seems more or less linear.

partitions <- list.files(path = "data/GBIF_snapshot_2022-11-01/occurrence.parquet/", full.names = TRUE)
partitions <- split(partitions, f = ceiling(seq_along(partitions)/20))

lapply(partitions[1], function(x) {
  parquet_path <- x
  tictoc::tic()
  
  occ <-
    arrow::open_dataset(sources = parquet_path) #works when calling online or local
  
  occ_all <- occ %>%
    select(year, decimallongitude, decimallatitude, species) %>%
    group_by(decimallongitude,
             decimallatitude,
             species,
             year) %>%  # remove duplicate rows
    filter(!is.na(species),!is.na(decimallatitude))  %>%
    summarize(records = n(),
              .groups = "drop") %>%
    collect()
  
  occ_all$cells <- occ_all %>%
    select(decimallatitude,
           decimallongitude) %>%
    h3::geo_to_h3(latlng = .,
                  res = 5)
  
  row_groups <- paste(basename(x[1]), 
                      basename(x[[length(x)]]), 
                      sep = "-")
  
  write_parquet(x = occ_all, paste0("output/indexed_occ/h3idx_occurence", row_groups, ".parquet"))
  tictoc::toc()
  
})

#The above works fine, but I don't think I'll be able to read them all back into memory and split them by h3 cell. 
# New approach is try to load the whole parquet and add the h3 index without other calculations.

# Test 
parquet_path <- "data/GBIF_snapshot_2022-11-01/occurrence.parquet/"
occ <- arrow::open_dataset(parquet_path)

#get column structure and names
arrow::open_dataset(sources = parquet_path)$schema
arrow::open_dataset(sources = parquet_path)$schema$names

tictoc::tic()

h3_idx <- occ %>%
  select(decimallatitude,
         decimallongitude) %>%
  collect() %>% 
  as.matrix() 
#  h3::geo_to_h3(latlng = .,
#                res = 1)

tictoc::toc()