#Compare diversity indices and ES50 vs ES100

library(ggplot2)
library(dplyr)

ES50 <- readRDS(file = "output/tests/ES50_CONUS/2010_res_1-6_bioidx.rds") %>% 
  data.table::rbindlist(idcol="resolution")

ES100 <- readRDS(file = "output/tests/ES100_CONUS/2010_res_1-6_bioidx.rds") %>% 
  data.table::rbindlist(idcol="resolution")

all <- tibble::lst(ES50, ES100) %>% 
  data.table::rbindlist(idcol="ES_depth")

rm(ES50)
rm(ES100)


#Compare ES100 to ES50

ggplot(data = all,
       aes(x = ES_depth,
           y = es)) +
  geom_boxplot() +
  facet_wrap(~ resolution) +
  labs(title = "Expected Species at H3 resolutions 1-6",
       caption = "Data from GBIF, CONUS 2010 - 2019")

ggsave(filename = "output/tests/comparison_of_indices/ES50vES100.png", width = 8, height = 6, units = "in")

#Compare ES to Shannon

ggplot(data = all,
       aes(x = es,
           y = shannon,
       color = ES_depth)) +
  geom_point() +
  facet_wrap(~ resolution) +
  labs(title = "Expected Species vs Shannon at H3 resolutions 1-6",
       caption = "Data from GBIF, CONUS 2010 - 2019")

ggsave(filename = "output/tests/comparison_of_indices/ESvShannon.png", width = 8, height = 6, units = "in")

#Compare ES to Hill 1

ggplot(data = all,
       aes(x = es,
           y = hill_1,
           color = ES_depth)) +
  geom_point() +
  facet_wrap(~ resolution) +
  labs(title = "Expected Species vs Hill_1 at H3 resolutions 1-6",
       caption = "Data from GBIF, CONUS 2010 - 2019")

ggsave(filename = "output/tests/comparison_of_indices/ESvHill1.png", width = 8, height = 6, units = "in")


#Compare number of records (n) to indices

p <- ggplot(data = all %>% 
         select(-maxp) %>%  
         tidyr::gather(key = "index", value = "value", sp:hill_2),
       aes(y = value,
           x = n,
           color = resolution)) +
  geom_point(size = 1) +
  scale_y_log10() +
  facet_wrap(~ index, 
             scales = "free_x") +
  labs(title = "Number of records vs Indices at H3 resolutions 1-6",
       caption = "Data from GBIF, CONUS 2010 - 2019")

ggsave(filename = "output/tests/comparison_of_indices/sampling_v_indices.png", width = 8, height = 6, units = "in")

#Same as above without log10 y-axis

#Compare number of records (n) to indices

p <- ggplot(data = all %>% 
              select(-maxp) %>%  
              tidyr::gather(key = "index", value = "value", sp:hill_2),
            aes(y = value,
                x = n,
                color = resolution)) +
  geom_point(size = 1) +
  facet_wrap(~ index, 
             scales = "free_x") +
  labs(title = "Number of records vs Indices at H3 resolutions 1-6",
       caption = "Data from GBIF, CONUS 2010 - 2019")

p

ggsave(filename = "output/tests/comparison_of_indices/sampling_v_indices_No_log10y.png", width = 8, height = 6, units = "in")

#lm of number of records to indices, at res 6 since it's the most data points. Can't lm betwen resolutions.

D <- all %>%
  select(-maxp) %>%
  filter(resolution == "res_6") %>%
  filter(ES_depth == "ES50") %>% 
  tidyr::gather(key = "index", value = "value", sp:hill_2)

idx <- levels(as.factor(D$index))

M <-lapply(idx, function(x){
  M <- lm(formula = get(x) ~ n, data = all)
  
  summary(M)[[4]]
  
})

names(M) <- idx

M

#Plot res 6 only


p <- ggplot(data = all %>% 
              filter(resolution == "res_6") %>%
              filter(ES_depth == "ES50") %>%
              select(-maxp) %>%  
              tidyr::gather(key = "index", value = "value", sp:hill_2),
            aes(y = value,
                x = n,
                color = resolution)) +
  geom_point(size = 1) +
  facet_wrap(~ index, 
             scales = "free_x") +
  labs(title = "Number of records vs Indices at H3 resolutions 1-6",
       caption = "Data from GBIF, CONUS 2010 - 2019")

p

ggsave(filename = "output/tests/comparison_of_indices/sampling_v_indices_No_log10y_res6only.png", width = 8, height = 6, units = "in")
