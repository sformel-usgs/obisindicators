##---- OBIS Download and Clean ----

#Downloaded most recent snapshot of OBIS and uploaded to HPC via GLOBUS

#Repartition by year and hemisphere (AKA quadrant) after indexing to h3

library(dplyr)
library(arrow)

setwd("/caldera/projects/css/sas/bio/spec_obs/global_biodiversity_map/")

#generate h3 indices by hemisphere
source("src/R/HPC/h3_index_by_hemisphere.R")

#Reduce parquet to columns that are needed for analysis
df <- open_dataset(sources = "data/raw/obis/snapshot/obis_20221006.parquet")

#select columns
desired_cols <- c("id",
                  "decimalLongitude",
                  "decimalLatitude",
                  "eventDate",
                  "date_year",
                  "countryCode",
                  "basisOfRecord",
                  "occurrenceStatus",
                  "flags",
                  "dropped",
                  "kingdom",
                  "class",
                  "species")

tictoc::tic()

occ <- df %>%
  select(all_of(desired_cols)) %>%
  filter(date_year >= 2010) %>%
  filter(date_year <= 2019) %>%
  filter(occurrenceStatus == "Present" | occurrenceStatus == "present") %>% #drops 3e5 records unless you account for inconsistent capitalization.
  filter(basisOfRecord == "Observation" |
           basisOfRecord == "MachineObservation" |
           basisOfRecord == "HumanObservation" |
           basisOfRecord == "MaterialSample" |
           basisOfRecord == "PreservedSpecimen" |
           basisOfRecord == "Occurrence") %>%
  filter(!is.na(species)) %>%
  filter(!is.na(decimalLatitude)) %>%
  select(-basisOfRecord,
         -occurrenceStatus,
         -countryCode) %>%
  collect()

tictoc::toc() #took 3 seconds

#From a quick look, OBIS will add about 3.3e5 records to the CONUS 2010s analysis.  But this will be reduced after merging with GBIF and duplicates are removed.
#Rename columns to match OBIS convention and write back out to parquet

occ <- occ %>%
  rename(obisid = id,
         decimallongitude = decimalLongitude,
         decimallatitude = decimalLatitude,
         year = date_year,
         eventdate = eventDate)

## ---- OBIS Index records to h3 and quadrants ----

#Add h3 index, hemisphere and rewrite as parquet

tictoc::tic()

occ$cell <- occ %>%
  select(decimallatitude,
         decimallongitude) %>%
  h3::geo_to_h3(latlng = .,
                res = 5)

occ$hemisphere <- case_when(occ$cell %in% NW_hex_ids ~ "NW",
                            occ$cell %in% NE_hex_ids ~ "NE",
                            occ$cell %in% SW_hex_ids ~ "SW",
                            occ$cell %in% SE_hex_ids ~ "SE")

tictoc::toc() #took 30 seconds

## ---- OBIS write parquet files ----
tictoc::tic()
write_dataset(dataset = occ,
              format = "parquet",
              path = "data/processed/OBIS_2010s/",
              partitioning = c("year","hemisphere"),
              existing_data_behavior = "overwrite")
tictoc::toc() #took 16 seconds
