% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/visualize.R
\name{gmap_indicator}
\alias{gmap_indicator}
\title{Statically map indicators using ggplot}
\usage{
gmap_indicator(
  grid,
  column = "shannon",
  label = "Shannon index",
  trans = "identity",
 
    crs = "+proj=robin +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
)
}
\arguments{
\item{grid}{spatial features, e.g. hexagons, to plot; requires a geometry
spatial column}

\item{column}{column name with indicator; default="shannon"}

\item{label}{label to show on legend}

\item{trans}{For continuous scales, the name of a transformation object or
the object itself. Built-in transformations include "asn", "atanh",
"boxcox", "date", "exp", "hms", "identity" (default), "log", "log10", "log1p",
"log2", "logit", "modulus", "probability", "probit", "pseudo_log",
"reciprocal", "reverse", "sqrt" and "time". See \code{ggplot2::continuous_scale}}

\item{crs}{coordinate reference system; see \code{sf::st_crs()}}
}
\value{
ggplot2 plot
}
\description{
Statically map indicators using ggplot
}
\concept{visualize}
