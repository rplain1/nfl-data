library(tidyverse)
library(duckdb)
con <- dbConnect(duckdb(), Sys.getenv("DB_PATH"))

dbListTables(con)


### Plays

df_plays <- tbl(con, Id("BASE", "NFLFASTR_PBP")) |>
  filter(play == 1, penalty == 0) |>
  mutate(head_coach = if_else(posteam == home_team, home_coach, away_coach)) |>
  group_by(
    season,
    week,
    game_id,
    posteam,
    head_coach,
    spread_line,
    total_line
  ) |>
  summarise(
    n = n(),
    passes = sum(pass),
    opp_pass_epa = mean(ifelse(pass == 1, epa, NA), na.rm = TRUE)
  ) |>
  mutate(pass_rate = passes / n)

df_passers <- tbl(con, Id("BASE", "NFLFASTR_PBP")) |>
  filter(!is.na(passer_id)) |>
  filter(play == 1, penalty == 0) |>
  count(season, week, game_id, posteam, passer_id, passer) |>
  collect() |>
  group_by(posteam, game_id) |>
  arrange(desc(n), .by_group = TRUE) |>
  mutate(rn = row_number()) |>
  filter(rn == 1) |>
  ungroup() |>
  select(-rn, -n)

assertthat::are_equal(df_passers |> count(game_id) |> pull(n) |> min(), 2)
assertthat::are_equal(df_passers |> count(game_id) |> pull(n) |> max(), 2)

df_plays_model <- df_plays |>
  collect() |>
  left_join(df_passers) |>
  group_by(posteam) |>
  arrange(season, week, .by_group = TRUE) |>
  mutate(
    new_coach = ifelse(head_coach == lag(head_coach), 0, 1),
    new_qb = ifelse(passer_id == lag(passer_id), 0, 1)
  ) |>
  ungroup() |>
  filter(season > 2010) |>
  rename(plays = n)


DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS SUMMARY")
DBI::dbWriteTable(
  con,
  DBI::Id("SUMMARY", "PLAY_COUNTS"),
  df_plays_model,
  overwrite = TRUE
)
DBI::dbDisconnect(con)
