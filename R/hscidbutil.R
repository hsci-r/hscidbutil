#' Delete all "temporary" tables starting with `tmp_` in the database
#' @param con the connection to probe
#' @export
#' @importFrom DBI dbGetQuery dbRemoveTable
delete_temporary_tables <- function(con) {
  for (tbl in dbGetQuery(con, "SHOW TABLES LIKE 'tmp_%'")[[1]]) {
    dbRemoveTable(con, tbl)
  }
}

#' Utility function to generate unique table names
#' @importFrom stats runif
unique_table_name <- function() {
  # Needs to use option to unique names across reloads while testing
  i <- getOption("tmp_table_name", floor(runif(1) * 1000) * 1000) + 1
  options(tmp_table_name = i)
  sprintf("tmp_%03i", i)
}

#' Utility finalizer function to remove ColumnStore temporary tables
#' @importFrom DBI dbExecute
#' @param fe finalizer environment containing connection (con) and table name (table_name)
delete_table_finalizer <- function(fe) {
  dbExecute(fe$con, paste0("DROP TABLE IF EXISTS ", fe$table_name))
}

#' ColumnStore version of [dplyr::compute()].
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param overwrite whether to overwrite existing tables (default `FALSE`)
#' @param temporary whether to create a temporary table (default `TRUE`)
#' @param ... Other arguments passed on to [dplyr::compute()],
#' @export
#' @importFrom dplyr filter compute
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
compute_c <- function(sql, name = unique_table_name(), overwrite = FALSE, temporary = TRUE, ...) {
  if (overwrite) dbExecute(sql$src$con, str_c("DROP TABLE IF EXISTS ", dbplyr::as.sql(name, sql$src$con)))
  engine <- dbGetQuery(sql$src$con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(sql$src$con, "SET SESSION storage_engine=Columnstore")
  r <- sql %>%
    filter(0L == 1L) %>%
    compute(name = dbplyr::as.sql(name, sql$src$con), temporary = FALSE, ...)
  dbExecute(sql$src$con, str_c("INSERT INTO ", dbplyr::as.sql(name, sql$src$con), " ", sql %>% dbplyr::remote_query()))
  dbExecute(sql$src$con, str_c("SET SESSION storage_engine=", engine))
  if (temporary == TRUE) {
    fe <- new.env(parent = emptyenv())
    fe$con <- r$src$con
    fe$table_name <- as.character(r$lazy_query$x)
    attr(r, "finalizer_env") <- fe
    reg.finalizer(fe, delete_table_finalizer, onexit = TRUE)
  }
  r
}

#' Version of [dplyr::compute()] that creates Aria tables.
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param overwrite whether to overwrite existing tables (default `FALSE`)
#' @param ... Other arguments passed on to [dplyr::compute()],
#' @export
#' @importFrom dplyr compute
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
compute_a <- function(sql, name = unique_table_name(), overwrite = FALSE, ...) {
  if (overwrite) dbExecute(sql$src$con, str_c("DROP TABLE IF EXISTS ", dbplyr::as.sql(name, sql$src$con)))
  engine <- dbGetQuery(sql$src$con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(sql$src$con, "SET SESSION storage_engine=Aria")
  r <- sql %>%
    compute(name = dbplyr::as.sql(name, sql$src$con), ...)
  dbExecute(sql$src$con, str_c("SET SESSION storage_engine=", engine))
  r
}

#' Version of [dplyr::copy_to()] that creates ColumnStore tables and has a better parameter order
#' @param df the dataframe to copy to the SQL store
#' @param con the connection to the SQL store
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param temporary whether to create a temporary table (default `TRUE`)
#' @param ... Other arguments passed on to [dplyr::copy_to()],
#' @export
#' @importFrom dplyr copy_to
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
copy_to_c <- function(df, con, name = unique_table_name(), temporary = TRUE, ...) {
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Columnstore")
  r <- copy_to(con, df, name = name, temporary = FALSE, ...)
  dbExecute(con, str_c("SET SESSION storage_engine=", engine))
  if (temporary == TRUE) {
    fe <- new.env(parent = emptyenv())
    fe$con <- r$src$con
    fe$table_name <- as.character(r$lazy_query$x)
    attr(r, "finalizer_env") <- fe
    reg.finalizer(fe, delete_table_finalizer, onexit = TRUE)
  }
  r
}

#' Version of [dplyr::copy_to()] that creates Aria tables and has a better parameter order
#' @param df the dataframe to copy to the SQL store
#' @param con the connection to the SQL store
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param ... Other arguments passed on to [dplyr::copy_to()],
#' @export
#' @importFrom dplyr copy_to
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
copy_to_a <- function(df, con, name = unique_table_name(), ...) {
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Aria")
  r <- copy_to(con, df, name = name, ...)
  dbExecute(con, str_c("SET SESSION storage_engine=", engine))
  r
}
