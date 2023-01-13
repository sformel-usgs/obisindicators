#ES50 calculated for United STates on USGS HPC Denali
#Based off original code from Abby Benson: use_gbif_parquet_file.rmd

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

library(obisindicators)
library(dplyr)
library(sf)
library(arrow)
library(magick)
library(ggplot2)
library(dggridR)

# get GBIF records
parquet_path <- "s3://gbif-open-data-us-east-1/occurrence/2022-07-01/occurrence.parquet"
occ <- arrow::open_dataset(parquet_path)

# In OBIS it's date_year and it's calculated for all records I believe, I'm not
# sure if the same is true for year in GBIF- how do we check?

begin <- Sys.time()
occ_all <- occ %>%	
  filter(countrycode == "US",
         kingdom == "Fungi",
         !is.na(decimallongitude)) %>% 
  group_by(decimallongitude, 
           decimallatitude, 
           species, 
           year) %>%  # remove duplicate rows
  filter(!is.na(species))  %>%
  summarize(records = n(),
            .groups = "drop") %>%
  collect()

end <- Sys.time()

#Print runtime
end - begin

#Create list of queries by decade
decades <- seq(from = 1960, to = 2020, by = 10) %>% 
  as.list()

occ_by_decade <- lapply(decades, function(x) {
  occ_all %>%
    filter(year >= x,
           year <= x + 9)
})


## Below is from ddgridr. Should we translate to h3?
# Create function to make grid, calculate metrics for different resolution grid sizes

res_changes <- function(occur, resolution = 9) {
  dggs <-
    dgconstruct(projection = "ISEA",
                topology = "HEXAGON",
                res = resolution)
  occur$cell <-
    dgGEO_to_SEQNUM(dggs, occur$decimallongitude, occur$decimallatitude)[["seqnum"]]
  idx <- calc_indicators(occur)
  
  grid <- dgcellstogrid(dggs, idx$cell) %>%
    st_wrap_dateline() %>%
    rename(cell = seqnum) %>%
    left_join(idx,
              by = "cell")
  return(grid)
}

## plot stuff
grid_list <- lapply(occ_by_decade, res_changes, resolution = 6)

names(grid_list) <- as.character(c(decades))

#There are some explicit arguments in hear that are needed for the HPC but aren't needed on most personal computers.

lapply(names(grid_list), function(x) {
  gmap_indicator(grid_list[[x]], "es", label = "ES(50)") %>%
    ggsave(plot = .,
           filename = paste0("/home/sformel/ES50_gbif/images/map_gbif_es_USA_", x, ".png"),
           width = 4,
           height = 4,
           units = "in",
           scale = 1,
           device = "png")
})

## create animated gif
decade_images <- list.files(path = "ES50_gbif/images", pattern = "*.png", full.names = TRUE)
img <- lapply(decade_images, image_read) %>% 
  image_join()

image_append(image_scale(img, "x200"))

gbif_es50_gif <- image_animate(image_scale(img, "1200x1200"), fps = 1, dispose = "previous") 
image_write(gbif_es50_gif, "/home/sformel/ES50_gbif/images/gbif_es50_animated_USA.gif")
