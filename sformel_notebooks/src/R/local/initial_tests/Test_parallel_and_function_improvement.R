#Test parallel version of grid indexing and diversity index calculation

#ES50 calculated for United STates on USGS HPC Denali
#Based off original code from Abby Benson: use_gbif_parquet_file.rmd

#obis indicators still in development, installed on 2022-10-26
#remotes::install_local("marinebon/obisindicators")

#This helped: https://data-blog.gbif.org/post/shapefiles/

# We kept running into memory limitations. While we're figuring that out, we decided to focus on maps that showed decades in the CONUS for discussion with USGS management.

library(obisindicators)
library(readr)
library(dplyr)
library(sf)

source("src/h3_grid_parallel.R")
source("src/h3_grid.R")
source("src/make_hex_res_parallel.R")
source("src/obis_calc_indic_improved.R")

#Create list of queries by decade
decades <- seq(from = 1990, to = 1999, by = 10)

#list CSV files generated on USGS HPC

lf <- list.files("data/processed/gbif_usa_1969_2022/", pattern = "*.csv", full.names = TRUE)
#lf <- list.files("/caldera/projects/css/sas/bio/spec_obs/gbif/output/", pattern = "^gbif_USA_.*.csv", full.names = TRUE)

#make list for map
dec_trimmed <- strtrim(decades, 3)

#bounding box for Contiguous US (CONUS) + buffer

West_Bounding_Coordinate <- -124.211606 - 0.5
East_Bounding_Coordinate <- -67.158958 - 0.5
North_Bounding_Coordinate <- 49.384359 - 0.5
South_Bounding_Coordinate <- 25.837377 - 0.5

tictoc::tic()

#read in csv made in HPC and filter to bounding box of CONUS, remove unnecessary columns
occ_by_decade <- lapply(dec_trimmed, function(x){
  file_names <- lf[grepl(lf, pattern = paste0("gbif_USA_",x,"\\d.csv"))]
  do.call(rbind, lapply(file_names, function(x){
    read_csv(x) %>%
      select(-kingdom, -class) %>% 
      filter(!is.na(decimallongitude)) %>% 
      filter(!is.na(species)) %>% 
      filter(decimallatitude < North_Bounding_Coordinate) %>% 
      filter(decimallatitude > South_Bounding_Coordinate) %>%
      filter(decimallongitude < East_Bounding_Coordinate) %>%
      filter(decimallongitude > West_Bounding_Coordinate)
  }))
})

tictoc::toc()


#fread, should be faster and use less memory, let's try that. Yes, it's about 3 times faster.

tictoc::tic()

occ_by_decade <- lapply(dec_trimmed, function(x){
  file_names <- lf[grepl(lf, pattern = paste0("gbif_USA_",x,"\\d.csv"))]
  do.call(rbind, lapply(file_names, function(x){
    data.table::fread(x) %>%
      select(-kingdom, -class) %>% 
      filter(!is.na(decimallongitude)) %>% 
      filter(!is.na(species)) %>% 
      filter(decimallatitude < North_Bounding_Coordinate) %>% 
      filter(decimallatitude > South_Bounding_Coordinate) %>%
      filter(decimallongitude < East_Bounding_Coordinate) %>%
      filter(decimallongitude > West_Bounding_Coordinate)
  }))
})

tictoc::toc()

#add decades as names
names(occ_by_decade) <- decades



#BEGIN TESTING------

#Serial
tictoc::tic()

# map defaults
RES <- 3
esn <- 50
column <- "es"
label <- paste0("ES", esn)
trans <- "identity"
crs <- "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,esn)

# make grid
grid_dec <- res_changes(occ = occ_by_decade[[1]],
                        resolution = RES, esn)

# make map
map <- gmap(
  grid_dec,
  column,
  label = label,
  trans = trans,
  crs = crs,
  limits = limits) +
  labs(caption = )

map 

tictoc::toc()


#Parallel
tictoc::tic()

# map defaults
RES <- 3
esn <- 50
column <- "es"
label <- paste0("ES", esn)
trans <- "identity"
crs <- "+proj=merc +lat_ts=0 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +type=crs"
limits <- c(0,esn)

# make grid
grid_dec <- res_changes_parallel(occ = occ_by_decade[[1]],
                                 resolution = RES, esn)

# make map
map <- gmap(
  grid_dec,
  column,
  label = label,
  trans = trans,
  crs = crs,
  limits = limits) +
  labs(caption = )

map 

tictoc::toc()

#Pull out just the grid part -in parallel

tictoc::tic()

future::plan(strategy = "multisession", workers = future::availableCores()*0.75)

occ <- occ_by_decade[[1]]

chunk <- 1000000
n <- nrow(occ)
r  <- rep(1:ceiling(n/chunk),each=chunk)[1:n]
occ <- split(occ,r)

occ <- furrr::future_map(occ, function(a){
  a %>%
    mutate(
      cell = h3::geo_to_h3(
        data.frame(decimallatitude,decimallongitude),
        res = 3
      )
    )
}, .options = furrr::furrr_options(seed = NULL))

tictoc::toc()

#This is the time consuming part.
tictoc::tic()
idx <- obisindicators::calc_indicators(occ)
tictoc::toc()

#FASTER WAY-----

res <- c(5)


#Make each grid and save the H3 indices (res 6 takes 30 min, make an RDS and import).  

tictoc::tic()

occ <- occ_by_decade[[1]] %>% 
  select(decimallatitude,
         decimallongitude)

cells <- lapply(res, function(x){
  h3::geo_to_h3(latlng = occ,
                res = x)
})

names(cells) <- paste0("res_", res)

tictoc::toc()

#make OBIS grids for each resolution in parallel - Saves some time (~ 25%) but starts taking a long time above res 3. 
#Should we just create these and package them with the obisindicators package? But even size 2 ~ 38 MB.

tictoc::tic()
future::plan(strategy = "multisession", workers = future::availableCores()*0.75)

hex <- furrr::future_map(res, function(x){
  obisindicators::make_hex_res(x)
},  .options = furrr::furrr_options(seed = NULL))

future::plan(strategy = "sequential")

tictoc::toc()

#serial for comparison

tictoc::tic()

hex <- lapply(res, function(x){
  obisindicators::make_hex_res(x)
})

tictoc::toc()

#Experiment with H3 grid generation

hex_res <-  2

tictoc::tic()

  CRS <- sf::st_crs(4326)
  east <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = 0,
      xmax = 180,
      ymin = -90,
      ymax = 90
    ), crs = CRS
  )))
  west <- sf::st_sf(geom = sf::st_as_sfc(sf::st_bbox(
    c(
      xmin = -180,
      xmax = 0,
      ymin = -90,
      ymax = 90
    ), crs = CRS
  )))
  hex_ids <-
    c(h3::polyfill(east, res = hex_res),
      h3::polyfill(west,
                   res = hex_res))

  dl_offset <- 60
  
tictoc::toc()

#This is the heavy section, what if we chunk up the hex_ids?  It take 1/7 the time!
tictoc::tic()    
  
  future::plan(strategy = "multisession",
               workers = future::availableCores() * 0.75)
  
  chunk <- 1000
  n <- length(hex_ids)
  r  <- rep(1:ceiling(n/chunk),each=chunk)[1:n]
  hex_ids <- split(hex_ids,r)
  
  hex_sf <-
    furrr::future_map_dfr(hex_ids, function(x){
        h3::h3_to_geo_boundary_sf(unlist(x))},
        .options = furrr::furrr_options(seed = NULL)) %>%
    sf::st_as_sf(data.table::rbindlist(.)) %>%
    sf::st_wrap_dateline(c(
      "WRAPDATELINE=YES",
      glue::glue("DATELINEOFFSET={dl_offset}")
    )) %>%
    dplyr::mutate(hexid = hex_ids) #Not sure what this is about?  I'm guessing, it was meant to use rename here. I've changed it in the parallel version..
  
  future::plan(strategy = "sequential")

tictoc::toc()


#How long does making grids 1:6 take on a medium grade laptop?

#Wow, jumping from size 4 to 5 increases the total time by 5x and the volume to 2 GB. 
#So, memory for a global grid is:
  #res 3 == 35 MB
  #res 4 == 244.6 MB
  #res 5 == 1.7 GB
  #res 6 == 12 GB (and took ~ 30 min on my laptop, ~ 15 times longer than res5)

#This makes sense, each level is basically an order of magnitude in terms of number of cells: https://h3geo.org/docs/core-library/restable/

#The chunk size could be increased on a bigger machine and probably run this whole thing faster, so chunksize is probably an important parameter.
#For my laptop, 2e5 rows seems to be a sweet spot.
res <- c(6)

tictoc::tic()

hex <- lapply(res, function(x){
  make_hex_res_parallel(x)
})

tictoc::toc()

#saving object so I can reload in the future. 
saveRDS(object = hex,file = "output/h3_grids/h3_res6_globalgrid.rds")

#Not sure if shapefile is any better, or if it will maintain projection, etc...received warning: 
#In CPL_write_ogr(obj, dsn, layer, driver, as.character(dataset_options),  :
#                   GDAL Message 1: 2GB file size limit reached for output/h3_grids\h3_res6_globalgrid.shp. Going on, but might cause compatibility issues with third party software

st_write(obj = hex[[1]], "output/h3_grids/h3_res6_globalgrid.shp")

#Then calculate the diversity indices in parallel------

tictoc::tic()

future::plan(strategy = "multisession", workers = future::availableCores()*0.75)
occ <- occ_by_decade[[1]]
occ <- cbind(occ, cell = cells[[3]])
occ <- dplyr::group_split(occ, cell)
idx_orig <- furrr::future_map(occ, function(a) {
  obisindicators::calc_indicators(a)
}, .options = furrr::furrr_options(seed = NULL)) %>%
  data.table::rbindlist()

future::plan(strategy = "sequential")

tictoc::toc()

#1990s at res 3 took 36 seconds

#compare to serial

tictoc::tic()

occ <- occ_by_decade[[1]]

occ <- cbind(occ, cell = cells[[3]])

idx <- obisindicators::calc_indicators(occ)

tictoc::toc()

#1990s at res 3 took 72 seconds

#What about with an improved version of the function?

tictoc::tic()

future::plan(strategy = "multisession", workers = future::availableCores()*0.75)
occ <- occ_by_decade[[1]]
occ <- cbind(occ, cell = cells[[3]])
occ <- dplyr::group_split(occ, cell)

idx_improved <- furrr::future_map(occ, function(a) {
  obis_calc_indicators_improved(a)
}, .options = furrr::furrr_options(seed = NULL)) %>%
  data.table::rbindlist()

future::plan(strategy = "sequential")

tictoc::toc()

#Does the output match the original function output?
all.equal(idx_orig, idx_improved)

#Is it faster?

#At res 1, the differences are negligible, but at res 3 it takes about 2/3 of the time

#Last part! Join grid and indicators-----

#This is pretty fast, probably doesn't need to be improved
tictoc::tic()
grid <- h3_grids[[5]] %>%
inner_join(
  idx[[5]],
  by = c("hex_ids" = "cell")
)
tictoc::toc()

#All together#----
#Read in data-----

library(obisindicators)
library(readr)
library(dplyr)
library(sf)

source("src/h3_grid_parallel.R")
source("src/h3_grid.R")
source("src/make_hex_res_parallel.R")

#Create list of queries by decade
decades <- seq(from = 1990, to = 1999, by = 10)

#list CSV files generated on USGS HPC

lf <- list.files("data/processed/gbif_usa_1969_2022/", pattern = "*.csv", full.names = TRUE)
#lf <- list.files("/caldera/projects/css/sas/bio/spec_obs/gbif/output/", pattern = "^gbif_USA_.*.csv", full.names = TRUE)

#make list for map
dec_trimmed <- strtrim(decades, 3)

#bounding box for Contiguous US (CONUS) + buffer

West_Bounding_Coordinate <- -124.211606 - 0.5
East_Bounding_Coordinate <- -67.158958 - 0.5
North_Bounding_Coordinate <- 49.384359 - 0.5
South_Bounding_Coordinate <- 25.837377 - 0.5

tictoc::tic()

#read in csv made in HPC and filter to bounding box of CONUS, remove unnecessary columns
occ_by_decade <- lapply(dec_trimmed, function(x){
  file_names <- lf[grepl(lf, pattern = paste0("gbif_USA_",x,"\\d.csv"))]
  do.call(rbind, lapply(file_names, function(x){
    read_csv(x) %>%
      select(-kingdom, -class) %>% 
      filter(!is.na(decimallongitude)) %>% 
      filter(!is.na(species)) %>% 
      filter(decimallatitude < North_Bounding_Coordinate) %>% 
      filter(decimallatitude > South_Bounding_Coordinate) %>%
      filter(decimallongitude < East_Bounding_Coordinate) %>%
      filter(decimallongitude > West_Bounding_Coordinate)
  }))
})

tictoc::toc()

#add decades as names
names(occ_by_decade) <- decades

#What resolutions to run?----
res <- c(3)

#Index data points to h3----
#What is returned is the list of h3 indices for each row in occurrences
# Res 1-5 takes about 40 seconds on my laptop for the 1960s

tictoc::tic()

occ <- occ_by_decade[[1]] %>% 
  select(decimallatitude,
         decimallongitude)

cells <- lapply(res, function(x){
  h3::geo_to_h3(latlng = occ,
                res = x)
})

names(cells) <- paste0("res_", res)

tictoc::toc()

#Make global h3 grids-----
#(res 6 takes 30 min, make once, save as an RDS and import).
# resolution 1 to 5 takes about 2.5 min on my laptop.

tictoc::tic()    
h3_grids <- lapply(res, make_hex_res_parallel)
tictoc::toc()

#calculate diversity indices----
#This is probably the longest part of this process
#resolutions 1:5 took about 9 min on my laptop computer

#One insight is that we can drop the columns that aren't needed (coordinates).

#Res 3 for 2010s took about 93 seconds on my computer

tictoc::tic()

occ <- occ_by_decade[[1]] %>% head(n = 1e6)

idx <- lapply(cells, function(x){

  future::plan(strategy = "multisession", workers = future::availableCores()*0.75)
  
  occ <- cbind(occ, "cell" = unlist(head(x, n = 1e6)))
  
  occ <- dplyr::group_split(occ, cell)
  
  b <- furrr::future_map(occ, function(a) {
    
    a %>% 
      as.data.frame() %>%
      select(-decimallatitude, -decimallongitude,) %>% 
      obis_calc_indicators_improved()
  }, .options = furrr::furrr_options(seed = NULL)) %>%
    data.table::rbindlist()
  
  return(b)
  future::plan(strategy = "sequential")

})

tictoc::toc()


# What if we convert to data table? At res 6, this really starts to bog down.

#obisindicators::calc_indicators() rewritten in data.table for speed

obis_calc_indicators_improved_dt <- function (df, esn = 50)
{
  stopifnot(is.data.frame(df))
  stopifnot(is.numeric(esn))
  stopifnot(all(c("cell", "species", "records") %in% names(df)))
  dt <- data.table::as.data.table(df)
  
  dt <- dt[, .(ni = sum(records)), by = .(species, cell)]
  
  dt[, `:=`(n = sum(ni))]  
  
  dt[, `:=`(hi = -(ni / n * log(ni / n)),
            si = (ni / n) ^ 2,
            qi = ni / n,
            esi = ifelse(n - ni >= esn, 1 - exp(gsl::lngamma(n - ni + 1) + gsl::lngamma(n - esn + 1) - gsl::lngamma(n - ni - esn + 1) - gsl::lngamma(n + 1)),
                         1)),] 
  dt <- dt[, .(n = sum(ni),
               sp = .N,
               shannon = sum(hi),
               simpson = sum(si),
               maxp = max(qi),
               es = sum(esi)), by = .(cell)]
  dt[, `:=`(hill_1 = exp(shannon),
            hill_2 = 1 / simpson,
            hill_inf = 1 / maxp)]
  
  return(dt)
  
}

#new function speed test
tictoc::tic()

occ <- occ_by_decade[[1]] %>% head(n = 1e6)

idx <- lapply(cells, function(x){
  
  future::plan(strategy = "multisession", workers = future::availableCores()*0.75)
  
  occ <- cbind(occ, "cell" = unlist(head(x, n = 1e6)))
  
  occ <- dplyr::group_split(occ, cell)
  
  b <- furrr::future_map(occ, function(a) {
    
    a %>%
      select(-decimallatitude, -decimallongitude,) %>% 
      obis_calc_indicators_improved_dt()
  }, .options = furrr::furrr_options(seed = NULL)) %>%
    data.table::rbindlist()
  
  return(b)
  future::plan(strategy = "sequential")
  
})

tictoc::toc()


#Actually takes longer, which might have to do with the parallel side of this.  Let's try in serial. Holy crap, it ran in 0.08 seconds. 

#Yes absolutely faster! by a ton.  So, this is good.  No need to run in parallel, but should used the data table version..

#new function speed test
tictoc::tic()

occ <- occ_by_decade[[1]] %>% head(n = 1e6)
occ <- cbind(occ, "cell" = unlist(head(cells[[1]], n = 1e6)))

NEW <- occ %>%
      select(-decimallatitude, -decimallongitude,) %>% 
      obis_calc_indicators_improved_dt()
  
tictoc::toc() #res 5, 2010s, first 1e6 rows == 0.72 seconds

#old function speed test

tictoc::tic()

occ <- occ_by_decade[[1]] %>% head(n = 1e6)
occ <- cbind(occ, "cell" = unlist(head(cells[[1]], n = 1e6)))

OLD <- occ %>%
  as.data.frame() %>% 
  select(-decimallatitude, -decimallongitude) %>% 
  obisindicators::calc_indicators()

tictoc::toc() #res 5, 2010s, first 1e6 rows == 52.3 seconds

#Are new and old identical products?

NEW1 <- as.data.frame(NEW[order(cell)], )
OLD1 <- as.data.frame(OLD) %>%  arrange(cell)

all.equal(NEW1,OLD1)

#Weird, functions aren't working as expected
NEW1[which(NEW1$es != OLD1$es),] %>% head()
OLD1[which(NEW1$es != OLD1$es),] %>% head()

NEW1[which(NEW1$es != OLD1$es),] %>% pull(es) %>% head()
OLD1[which(NEW1$es != OLD1$es),] %>% pull(es) %>%  head()

#look at particular row
NEW1[29,]
OLD1[29,]

#So clearly can't try the != comparison above. It's because NA == NA returns NA, not TRUE.
#https://stackoverflow.com/questions/16822426/dealing-with-true-false-na-and-nan

compareNA <- function(v1,v2) {
  # This function returns TRUE wherever elements are the same, including NA's,
  # and false everywhere else.
  same <- (v1 == v2)  |  (is.na(v1) & is.na(v2))
  same[is.na(same)] <- FALSE
  return(same)
}

NEW1[!compareNA(NEW1$es,OLD1$es),] %>% head()
OLD1[!compareNA(NEW1$es,OLD1$es),] %>% head()

#Still not right.
A <- as.vector(NEW1$es)
B <- as.vector(OLD1$es)

compareNA(A,B)

#Maybe we're not seeing all the decimals?
options(digits = 15)  

A[29]
B[29]

#That's it. Ok, now let's see what' up with those extra NAs.
OLD2 <- OLD1[is.na(OLD1$es),]
NEW2 <- NEW1[is.na(NEW1$es),]

wrong_cells <- NEW2$cell[!(NEW2$cell %in% OLD2$cell)]

NEW1[NEW1$cell %in% wrong_cells,] %>%  head()
OLD1[NEW1$cell %in% wrong_cells,] %>%  head()

#Subset data to those cells and work through it by hand.
df <- occ %>%
  as.data.frame() %>% 
  select(-decimallatitude, -decimallongitude) %>%
  filter(cell %in% wrong_cells) %>% 
  arrange(cell, species)

#Ended up being that the ifelse statement for esi wasn't written correctly (i.e. something needed to be made explicit).

#join grid and indicators----

tictoc::tic()
grid <- lapply(h3_grids, function(x){
  x %>%
  inner_join(
    idx[[5]],
    by = c("hex_ids" = "cell"))
})
tictoc::toc()
