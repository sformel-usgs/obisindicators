#Download most recent snapshot of OBIS and repartition by year and hemosphere after indexing to h3

library(dplyr)
library(arrow)

#release_date <- 20221006
#url <- paste0("https://obis-datasets.ams3.digitaloceanspaces.com/exports/obis_", release_date, ".parquet")

#options(timeout=3000) # needed if it takes longer than 60 seconds to download
#download.file(url = url, destfile = paste0("data/", basename(url)))


#output filepath for voluminous data
data_path <- "C:/Users/sformel/Downloads/heavy_data_not_synced/global_biodiversity_map/"


#list all partitions
partitions <- list.files(path = paste0(data_path,
                                       "raw/GBIF_snapshot/2022-12-01/occurrence.parquet/"), full.names = TRUE)

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
              paste0(data_path,
                     "processed/GBIF_2010s/",
                     partition_name),
              partitioning = c("year", "hemisphere"), 
              existing_data_behavior = "overwrite")

tictoc::toc()

})


time_total <- tictoc::toc() #took about 42 min on my laptop

