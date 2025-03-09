con <- DBI::dbConnect(duckdb(), Sys.getenv("DB_PATH"))

### Game lines

dplyr::tbl(con, DBI::Id("BASE", "NFLFASTR_PBP")) |>
  dplyr::group_by(season, week, game_id, season_type, home_team, away_team) |>
  dplyr::summarise(
    spread_line = max(spread_line),
    total_line = max(total_line),
    home_score = max(home_score),
    away_score = max(away_score),
    result = max(result),
    .groups = 'drop'
  ) |>
  dplyr::collect() -> df_base

df_stage <- df_base |>
  tidyr::pivot_longer(
    cols = c(home_team, away_team),
    names_to = 'home_away',
    values_to = 'team'
  ) |>
  dplyr::mutate(
    outcome = dplyr::case_when(
      result == 0 ~ 'TIE',
      home_away == 'home_team' & result > 0 ~ 'WIN',
      home_away == 'away_team' & result < 0 ~ 'WIN',
      home_away == 'home_team' & result < 0 ~ 'LOSS',
      home_away == 'away_team' & result > 0 ~ 'LOSS',
      TRUE ~ NA_character_
    ),
    score = dplyr::if_else(home_away == 'home_team', home_score, away_score),
    implied_score = dplyr::case_when(
      home_away == 'home_team' & spread_line > 0 ~
        (total_line + spread_line) / 2,
      home_away == 'away_team' & spread_line < 0 ~
        (total_line + spread_line) / 2,
      home_away == 'home_team' & spread_line < 0 ~
        (total_line - spread_line) / 2,
      home_away == 'away_team' & spread_line > 0 ~
        (total_line - spread_line) / 2,
      spread_line == 0 ~ total_line / 2,
      TRUE ~ NA_real_
    )
  ) |>
  dplyr::mutate(total_score = home_score + away_score) |>
  dplyr::select(
    season,
    week,
    game_id,
    season_type,
    team,
    home_away,
    spread_line,
    total_line,
    outcome,
    score,
    implied_score,
    total_score
  )

df <- df_stage |>
  tidyr::pivot_wider(
    id_cols = c(season, week, game_id, season_type),
    names_from = home_away,
    values_from = team
  ) |>
  dplyr::left_join(df_stage)

DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS SUMMARY")
DBI::dbWriteTable(
  con,
  DBI::Id("SUMMARY", "NFL_LINES_RESULTS"),
  df,
  overwrite = TRUE
)

tbl(con, DBI::Id("SUMMARY", "NFL_LINES_RESULTS"))

DBI::dbDisconnect(con)
