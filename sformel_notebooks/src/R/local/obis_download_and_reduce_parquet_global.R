#Download most recent snapshot of OBIS and repartition by year and hemosphere after indexing to h3

library(dplyr)
library(arrow)

#release_date <- 20221006
#url <- paste0("https://obis-datasets.ams3.digitaloceanspaces.com/exports/obis_", release_date, ".parquet")

#options(timeout=3000) # needed if it takes longer than 60 seconds to download
#download.file(url = url, destfile = paste0("data/", basename(url)))


#output filepath for voluminous data
data_path <- "C:/Users/sformel/Downloads/heavy_data_not_synced/global_biodiversity_map/"

#Reduce parquet to columns that are needed for analysis
df <- open_dataset(sources = paste0(data_path,
                                    "raw//obis_20221006.parquet"))


#check schema is being read correctly
df$schema

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

#From a quick look, OBIS will add about 3.3e5 records to the CONUS 2010s analysis.  But this will be reduced after merging with GBIF and duplicates are removed.
#Rename columns to match OBIS convention and write back out to parquet

occ <- occ %>% 
  rename(obisid = id,
         decimallongitude = decimalLongitude,
         decimallatitude = decimalLatitude,
         year = date_year,
         eventdate = eventDate)

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

tictoc::toc() #took 2 seconds

#write parquet files
write_dataset(dataset = occ,
              format = "parquet",
              path = paste0(data_path,
                            "processed/OBIS_2010s/"), 
              partitioning = c("year","hemisphere"), 
              existing_data_behavior = "overwrite")

