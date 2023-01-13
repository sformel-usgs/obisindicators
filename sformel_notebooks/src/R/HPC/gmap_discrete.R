##----gmap discrete function----

#Plot function, based off one written by Matt Biddle.  I extracted out some part he put into the function, so it would be easier to change them

#partly based on John Waller's plots: https://github.com/jhnwllr/es50


gmap_discrete <- function(grid,
                 column = "shannon",
                 label = "Shannon index",
                 trans = "identity",
                 crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs",
                 limits = c(0, 8)) {


  #bounding box for Contiguous US (CONUS) + buffer
  # West_Bounding_Coordinate <- -124.211606 - 0.5
  # East_Bounding_Coordinate <- -67.158958 - 0.5
  # North_Bounding_Coordinate <- 49.384359 - 0.5
  # South_Bounding_Coordinate <- 25.837377 - 0.5
  #
  # bb_roi <- paste('POLYGON ((',
  #                 West_Bounding_Coordinate,South_Bounding_Coordinate, ",",
  #                 East_Bounding_Coordinate, South_Bounding_Coordinate, ",",
  #                 East_Bounding_Coordinate, North_Bounding_Coordinate, ",",
  #                 West_Bounding_Coordinate, North_Bounding_Coordinate, ",",
  #                 West_Bounding_Coordinate, South_Bounding_Coordinate, '))')

  bb_roi <-  'POLYGON ((-180 -90, 180 -90, 180 90, -180 90, -180 -90))'

  sfc <- st_as_sfc(bb_roi, crs = '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0')
  bb <- sf::st_bbox(st_transform(sfc, crs))

  breaks = seq(0, max(limits), max(limits)/5)

  grid[[column]] <- cut(grid[[column]], breaks = breaks)

  #grid[[column]] <- as.factor(grid[[column]])

  #grid[[column]] <- addNA(grid[[column]])

  ggplot() +

    geom_sf(
      data = grid,
      aes_string(fill = column, geometry = "geometry"),
      show.legend = TRUE,
      #lwd = 0.01
      color = NA # https://github.com/tidyverse/ggplot2/issues/3744
    ) +
    scale_fill_brewer(palette = "Spectral", direction=-1, na.value = 'skyblue1') +

    geom_sf(
      data = world,
      fill = NA,
      color = "grey",
      lwd = 0.1
    ) +

    # geom_sf(
    #   data = USA_shape,
    #   fill = NA,
    #   color = "#000000",
    #   lwd = 0.1
    # ) +

    coord_sf(crs  = crs,
             xlim = bb[c("xmin", "xmax")],
             ylim = bb[c("ymin", "ymax")]) +

    theme(
      panel.background = element_rect(
        fill = "white",
        colour = "gray95",
        size = 0,
        linetype = "blank"
      ),
      axis.ticks = element_blank(),
      #axis.title.x = element_blank(),
      #axis.title.y = element_blank(),
      #axis.text = element_blank(),
      #panel.grid = element_blank(),
      plot.margin = margin(0, 0, 0, 0, "cm")
    )
}
