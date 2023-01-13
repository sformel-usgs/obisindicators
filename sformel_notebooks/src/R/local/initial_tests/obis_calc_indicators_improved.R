#This is intended to be run in parallel on a list of occurrences where they have been split into a list based on the cell occurrence.
#The major change is removing some of the grouping and cleaning up the code format so it's easier to read.

obis_calc_indicators_improved <- function (df, esn = 50)
{
  stopifnot(is.data.frame(df))
  stopifnot(is.numeric(esn))
  stopifnot(all(c("cell", "species", "records") %in% names(df)))
  df %>%
    group_by(species) %>%
    summarize(ni = sum(records),
              .groups = "drop_last") %>%
    mutate(n = sum(ni),
           hi = -(ni / n * log(ni / n)),
           #si = (ni / n) ^ 2,
           #qi = ni / n,
           esi = case_when(n - ni >= esn ~ 1 - exp(gsl::lngamma(n - ni + 1) + gsl::lngamma(n - esn + 1) - gsl::lngamma(n - ni - esn + 1) - gsl::lngamma(n + 1)),
                 n >= esn ~ 1)) %>% 
    summarize(
               n = sum(ni),
               sp = n(),
               shannon = sum(hi),
               #simpson = sum(si),
               #maxp = max(qi),
               es = sum(esi),
               .groups = "drop") %>% 
    mutate(
      hill_1 = exp(shannon),
      #hill_2 = 1 / simpson,
      #hill_inf = 1 / maxp,
      #cell = unique(df$cell) # a way to add cell back, so it isn't need during grouping.
    )
}
