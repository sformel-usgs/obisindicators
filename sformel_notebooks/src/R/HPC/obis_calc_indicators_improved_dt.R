##----calc_indicators as data.table----

#obisindicators::calc_indicators() rewritten in data.table for speed

obis_calc_indicators_improved_dt <- function (df, esn = 50)
{
  stopifnot(is.data.frame(df))
  stopifnot(is.numeric(esn))
  stopifnot(all(c("cell", "species", "records") %in% names(df)))
  dt <- data.table::as.data.table(df)

  dt <- dt[, .(ni = sum(records)),
           by = .(cell, species)]

  dt[, `:=`(n = sum(ni)),
     by = cell]

  dt[, `:=`(hi = -(ni / n * log(ni / n)),
            si = (ni / n) ^ 2,
            qi = ni / n,
            esi = ifelse(n - ni >= esn,
                         1 - exp(gsl::lngamma(n - ni + 1) + gsl::lngamma(n - esn + 1) - gsl::lngamma(n - ni - esn + 1) - gsl::lngamma(n + 1)),
                         ifelse(n >= esn,
                                1,
                                NA))),
     by = cell]

  dt <- dt[, .(n = sum(ni),
               sp = .N,
               shannon = sum(hi),
               simpson = sum(si),
               maxp = max(qi),
               es = sum(esi)), by = .(cell)]
  dt[, `:=`(hill_1 = exp(shannon),
            hill_2 = 1 / simpson,
            hill_inf = 1 / maxp),
     by = cell]

  return(dt)

}
