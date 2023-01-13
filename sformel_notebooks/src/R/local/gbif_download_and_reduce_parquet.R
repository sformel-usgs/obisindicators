#GBIF download and reduce parquet

library(dplyr)
library(arrow)

#download parquet from s3://gbif-open-data-us-east-1/occurrence/2022-11-01/occurrence.parquet/
#note that a new snapshot is generated every month
#use wget, aws cli, or whatever utility you like

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
  occ_all <- occ %>%
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
    filter(countrycode == "US") %>%
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
  
  
  #write to parquet
  partition_name <- basename(x)
  
  write_dataset(dataset = occ_all,
                paste0(data_path,
                       "processed/GBIF_USA_2010s/",
                  partition_name),
                partitioning = c("year"), 
                existing_data_behavior = "overwrite")
  
  tictoc::toc()
  
})


time_total <- tictoc::toc() #took about 10 min on my laptop

