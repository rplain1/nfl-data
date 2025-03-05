update_pbp_db <- function(dir = 'data', force_rebuild = FALSE) {
  nflfastR::update_db(
    dbdir = getOption("nflfastR.dbdirectory", default = "data"),
    dbname = "pbp_db.sqlite",
    tblname = "nflfastR_pbp",
    force_rebuild = FALSE,
    db_connection = NULL
  )
}
update_pbp_db()
