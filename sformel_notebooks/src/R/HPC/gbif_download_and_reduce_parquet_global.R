##---- GBIF download and process----

#Downloaded most recent snapshot of GBIF via GLBUS.

# Repartition by year and hemisphere after indexing to h3

library(dplyr)
library(arrow)

setwd("/caldera/projects/css/sas/bio/spec_obs/global_biodiversity_map/")

#generate h3 indices by hemisphere
source("src/R/HPC/h3_index_by_hemisphere.R")


#list all partitions
partitions <- list.files(path = "data/raw/gbif/snapshot/2023-01-01/occurrence.parquet/", full.names = TRUE)

tictoc::tic()

lapply(partitions, function(x) {

  print(x) #acts as a quasi-progress bar
  tictoc::tic()

  occ <- arrow::open_dataset(sources = x)

  # %in% and != aren't implemented yet in filter arrow
  occ <- occ %>%
    select(
      gbifid,
      #issue, #will be incorporated in the future, array is irritating to parse with R tools
      basisofrecord,
      occurrencestatus,
      countrycode,
      year,
      decimallongitude,
      decimallatitude,
      species,
      kingdom,
      class,
      eventdate
    ) %>%
    filter(year >= 2010) %>%
    filter(year <= 2019) %>%
    filter(occurrencestatus == "PRESENT") %>%
    filter(basisofrecord == "OBSERVATION" |
             basisofrecord == "MACHINE_OBSERVATION" |
             basisofrecord == "HUMAN_OBSERVATION" |
             basisofrecord == "MATERIAL_SAMPLE" |
             basisofrecord == "PRESERVED_SPECIMEN" |
             basisofrecord == "OCCURRENCE"
    ) %>%
    filter(!is.na(species)) %>%
    filter(!is.na(decimallatitude)) %>%
    select(-basisofrecord,-occurrencestatus,-countrycode) %>%
    collect()

#Add h3 index, hemisphere and rewrite as parquet

occ$cell <- occ %>%
  select(decimallatitude,
         decimallongitude) %>%
  h3::geo_to_h3(latlng = .,
                res = 5)

occ$hemisphere <- case_when(occ$cell %in% NW_hex_ids ~ "NW",
                            occ$cell %in% NE_hex_ids ~ "NE",
                            occ$cell %in% SW_hex_ids ~ "SW",
                            occ$cell %in% SE_hex_ids ~ "SE")


#write to parquet
partition_name <- basename(x)

write_dataset(dataset = occ,
              path = paste0("data/processed/GBIF_2010s/",
                     partition_name),
              partitioning = c("year", "hemisphere"),
              existing_data_behavior = "overwrite")

tictoc::toc()

})


time_total <- tictoc::toc() #took about 48 min on USGS HPC Denali. Each partition took 1 -3 seconds.

