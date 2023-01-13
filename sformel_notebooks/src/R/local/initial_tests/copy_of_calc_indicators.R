function (df, esn = 50)
{
  stopifnot(is.data.frame(df))
  stopifnot(is.numeric(esn))
  stopifnot(all(c("cell", "species", "records") %in% names(df)))
TEST <-   df %>%
    group_by(cell, species) %>%
    summarize(ni = sum(records),
              .groups = "drop_last") %>%
    mutate(n = sum(ni)) %>% group_by(cell,
                                     species) %>%
    mutate(
      hi = -(ni / n * log(ni / n)),
      si = (ni / n) ^ 2,
      qi = ni /
        n,
      esi = case_when(
        n - ni >= esn ~ 1 - exp(
          gsl::lngamma(n -
                         ni + 1) + gsl::lngamma(n - esn + 1) - gsl::lngamma(n -
                                                                              ni - esn + 1) - gsl::lngamma(n + 1)
        ),
        n >= esn ~
          1
      )
    ) %>%
    group_by(cell) %>%
    summarize(
      n = sum(ni),
      sp = n(),
      shannon = sum(hi),
      simpson = sum(si),
      maxp = max(qi),
      es = sum(esi),
      .groups = "drop"
    ) %>%
    mutate(
      hill_1 = exp(shannon),
      hill_2 = 1 /
        simpson,
      hill_inf = 1 / maxp
    )
}
