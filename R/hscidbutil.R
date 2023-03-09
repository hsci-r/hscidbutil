#' Get a database connection as defined by a yaml configuration
#' or environment variables
#' @param db_params a db_params.yaml-file that defines db_host, db_name and db_user
#' @param db_secret a db_secret.yaml-file that defines db_pass and optionally overrides other values
#' @param bigint how should the connection convert bigints
#' @param ... Other arguments passed on to [DBI::dbConnect()],
#' @export
#' @importFrom here here
#' @importFrom keyring key_set key_get
#' @importFrom yaml read_yaml
#' @importFrom RMariaDB MariaDB
#' @importFrom stringr str_c
#' @importFrom DBI dbConnect
get_connection <- function(db_params=here("db_params.yaml"),db_secret=here("db_secret.yaml"),bigint="integer", ...) {
  while (!exists("con", inherits=FALSE)) {
    db_host <- Sys.getenv("DB_HOST")
    db_name <- Sys.getenv("DB_NAME")
    db_user <- Sys.getenv("DB_USER")
    db_pass <- Sys.getenv("DB_PASS")
    if (file.exists(db_params)) {
      db_params_dict <- read_yaml(db_params)
      if (db_host=="" && !is.null(db_params_dict$db_host)) db_host <- db_params_dict$db_host
      if (db_name=="" && !is.null(db_params_dict$db_name)) db_name <- db_params_dict$db_name
      if (db_user=="" && !is.null(db_params_dict$db_user)) db_user <- db_params_dict$db_user
    }
    if (file.exists(db_secret)) {
      db_secret_dict <- read_yaml(db_secret)
      if (db_host=="" && !is.null(db_secret_dict$db_host)) db_host <- db_secret_dict$db_host
      if (db_name=="" && !is.null(db_secret_dict$db_name)) db_name <- db_secret_dict$db_name
      if (db_user=="" && !is.null(db_secret_dict$db_user)) db_user <- db_secret_dict$db_user
      if (db_pass=="" && !is.null(db_secret_dict$db_pass)) db_pass <- db_secret_dict$db_pass
    }
    if (db_host=="" || db_name=="") stop(str_c("Could not derive all required variables for database connectivity. Have db_host:",db_host,", db_name:",db_name))
    tryCatch(con <- dbConnect(
      drv = MariaDB(),
      host = db_host,
      dbname = db_name,
      user = if (db_user!="") db_user else key_get(db_name,"DB_USER"),
      password = if (db_pass!="") db_pass else key_get(db_name,"DB_PASS"),
      bigint = bigint,
      load_data_local_infile = TRUE,
      autocommit = TRUE,
      reconnect = TRUE,
      ...
    ), error = function(e) {
      print(e)
      key_set(db_name,"DB_USER", prompt="Database username: ")
      key_set(db_name,"DB_PASS", prompt="Database password: ")
    })
  }
  con
}

#' List schemas in a database
#' @param con the connection to probe
#' @export
#' @importFrom dplyr tbl pull
#' @importFrom dbplyr in_schema
list_schemas <- function(con) {
  tbl(con, dbplyr::in_schema("information_schema","SCHEMATA")) %>%
    pull("SCHEMA_NAME")
}

#' Register all tables in a schema as dbplyr tables in the given enviroment
#' @param con the connection to probe
#' @param schemas the schemas to probe
#' @param envir the environment in which to register the tables
#' @export
#' @importFrom dplyr tbl filter select collect
#' @importFrom purrr walk2
#' @importFrom dbplyr in_schema
register_tables <- function(con, schemas, envir=.GlobalEnv) {
  d <- tbl(con, dbplyr::in_schema("information_schema","TABLES")) %>%
    filter(TABLE_SCHEMA %in% schemas) %>%
    select(TABLE_SCHEMA, TABLE_NAME) %>%
    collect()
  walk2(d$TABLE_NAME, d$TABLE_SCHEMA, ~try(assign(.x, tbl(con, dbplyr::in_schema(.y, .x)), envir = envir)))
}

#' List all "temporary" tables starting with `tmp_` in the given schema
#' @param con the connection to probe
#' @param schemas the schemas to probe
#' @export
#' @importFrom dplyr tbl filter select collect
#' @importFrom dbplyr in_schema
#' @importFrom stringr str_detect
list_temporary_tables <- function(con, schemas) {
  tbl(con, dbplyr::in_schema("information_schema","TABLES")) %>%
    filter(TABLE_SCHEMA %in% schemas,str_detect(TABLE_NAME,"^tmp_")) %>%
    select(TABLE_SCHEMA,TABLE_NAME) %>%
    collect()
}

#' Delete all "temporary" tables starting with `tmp_` in the given schemas
#' @param con the connection to probe
#' @param schemas the schemas to probe
#' @export
#' @importFrom DBI dbRemoveTable Id
#' @importFrom purrr walk2
delete_temporary_tables <- function(con, schemas) {
  d <- list_temporary_tables(con, schemas)
  walk2(d$TABLE_SCHEMA,d$TABLE_NAME, ~dbRemoveTable(con, Id(schema=.x, table=.y)))
}

#' Utility function to generate unique table names
#' @keywords internal
#' @importFrom stats runif
unique_table_name <- function() {
  # Needs to use option to unique names across reloads while testing
  i <- getOption("tmp_table_name", floor(runif(1) * 1000) * 1000) + 1
  options(tmp_table_name = i)
  sprintf("tmp_%03i", i)
}

#' Utility finalizer function to remove ColumnStore temporary tables
#' @importFrom DBI dbExecute
#' @keywords internal
#' @param fe finalizer environment containing connection (con) and table name (table_name)
delete_table_finalizer <- function(fe) {
  dbExecute(fe$con, paste0("DROP TABLE IF EXISTS ", fe$table_name))
}

#' ColumnStore version of [dplyr::compute()].
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param overwrite whether to overwrite existing tables
#' @param temporary whether to create a temporary table
#' @param ... Other arguments passed on to [dplyr::compute()],
#' @export
#' @importFrom dplyr filter compute
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
compute_c <- function(sql, name = unique_table_name(), overwrite, temporary, ...) {
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
#' @param overwrite whether to overwrite existing tables
#' @param temporary whether to create a temporary table
#' @param ... Other arguments passed on to [dplyr::compute()],
#' @export
#' @importFrom dplyr compute
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
compute_a <- function(sql, name = unique_table_name(), overwrite, temporary, ...) {
  if (overwrite) dbExecute(sql$src$con, str_c("DROP TABLE IF EXISTS ", dbplyr::as.sql(name, sql$src$con)))
  engine <- dbGetQuery(sql$src$con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(sql$src$con, "SET SESSION storage_engine=Aria")
  r <- sql %>%
    compute(name = dbplyr::as.sql(name, sql$src$con), temporary = FALSE, ...)
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

#' Version of [dplyr::copy_to()] that creates ColumnStore tables and has a better parameter order
#' @param df the dataframe to copy to the SQL store
#' @param con the connection to the SQL store
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param temporary whether to create a temporary table
#' @param ... Other arguments passed on to [dplyr::copy_to()],
#' @export
#' @importFrom dplyr copy_to
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
copy_to_c <- function(df, con, name = unique_table_name(), ...) {
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
#' @param temporary whether to create a temporary table
#' @param ... Other arguments passed on to [dplyr::copy_to()],
#' @export
#' @importFrom dplyr copy_to
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
copy_to_a <- function(df, con, name = unique_table_name(), ...) {
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Aria")
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
