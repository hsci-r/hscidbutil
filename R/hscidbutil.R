library(DBI)
library(RMariaDB)
library(dbplyr)

#' Delete all "temporary" tables starting with `tmp_` in the database
#' @param con the connection to probe
#' @export
delete_temporary_tables <- function(con) {
  for (tbl in dbGetQuery(con,"SHOW TABLES LIKE 'tmp_%'")[[1]])
    dbRemoveTable(con,tbl)
}

#' Utility function to generate unique table names
unique_table_name <- function() {
  # Needs to use option to unique names across reloads while testing
  i <- getOption("tmp_table_name", floor(runif(1)*1000)*1000) + 1
  options(tmp_table_name = i)
  sprintf("tmp_%03i", i)
}

#' ColumnStore version of [dbplyr::compute()].
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param overwrite whether to overwrite existing tables (default `FALSE`)
#' @param ... Other arguments passed on to [dbplyr::compute()],
#' @export
compute_c <- function(sql, name = unique_table_name(), overwrite=FALSE, ...) {
  if (overwrite) dbExecute(sql$src$con, str_c("DROP TABLE IF EXISTS ",dbplyr::as.sql(name, sql$src$con)))
  engine <- dbGetQuery(sql$src$con,"SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Columnstore")
  r <- sql %>%
    filter(0L==1L) %>%
    compute(name=dbplyr::as.sql(name, sql$src$con), temporary=FALSE,...)
  dbExecute(sql$src$con, str_c("INSERT INTO ",dbplyr::as.sql(name, sql$src$con)," ",sql %>% dbplyr::remote_query()))
  dbExecute(sql$src$con, str_c("SET SESSION storage_engine=",engine))
  r
}

#' Version of [dbplyr::compute()] that creates Aria tables.
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param overwrite whether to overwrite existing tables (default `TRUE`)
#' @param ... Other arguments passed on to [dbplyr::compute()],
#' @export
compute_a <- function(sql, name = unique_table_name(),...) {
  dbExecute(sql$src$con, str_c("DROP TABLE IF EXISTS ",dbplyr::as.sql(name, sql$src$con)))
  engine <- dbGetQuery(sql$src$con,"SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Aria")
  r <- sql %>%
    compute(name=name, ...)
  dbExecute(sql$src$con, str_c("SET SESSION storage_engine=",engine))
  r
}

#' Version of [dbplyr::copy_to()] that creates ColumnStore tables and has a better parameter order
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param ... Other arguments passed on to [dbplyr::copy_to()],
#' @export
copy_to_c <- function(df,con, name = unique_table_name(),...) {
  engine <- dbGetQuery(con,"SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Columnstore")
  r <- copy_to(con,df,name=name,temporary=F,...)
  dbExecute(con, str_c("SET SESSION storage_engine=",engine))
  r
}

#' Version of [dbplyr::copy_to()] that creates Aria tables and has a better parameter order
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param ... Other arguments passed on to [dbplyr::copy_to()],
#' @export
copy_to_a <- function(df,con, name = unique_table_name(),...) {
  engine <- dbGetQuery(con,"SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Aria")
  r <- copy_to(con,df,name=name,...)
  dbExecute(con, str_c("SET SESSION storage_engine=",engine))
  r
}
