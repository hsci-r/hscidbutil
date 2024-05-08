#' Get a database connection as defined by a yaml configuration
#' or environment variables
#' @param params a params.yaml-file that defines some of db_host, db_name and db_user under a given key
#' @param secret a secret.yaml-file that defines some of db_host, db_name, db_user and db_pass under a given key
#' @param key the key in the yaml file to extract
#' @param bigint how should the connection convert bigints
#' @param ... Other arguments passed on to [DBI::dbConnect()],
#' @export
#' @importFrom here here
#' @importFrom yaml read_yaml
#' @importFrom RMariaDB MariaDB
#' @importFrom stringr str_c
#' @importFrom DBI dbConnect
#' @return the MariaDB connection object
get_connection <- function(params = here("params.yaml"), secret = here("secret.yaml"), key="db", bigint = "integer", ...) {
  db_host <- Sys.getenv("DB_HOST")
  db_name <- Sys.getenv("DB_NAME")
  db_user <- Sys.getenv("DB_USER")
  db_pass <- Sys.getenv("DB_PASS")
  if (file.exists(params)) {
    params_dict <- read_yaml(params)
    if (db_host == "" && !is.null(params_dict[[key]]$db_host)) db_host <- params_dict[[key]]$db_host
    if (db_name == "" && !is.null(params_dict[[key]]$db_name)) db_name <- params_dict[[key]]$db_name
    if (db_user == "" && !is.null(params_dict[[key]]$db_user)) db_user <- params_dict[[key]]$db_user
  }
  if (file.exists(secret)) {
    secret_dict <- read_yaml(secret)
    if (db_host == "" && !is.null(secret_dict[[key]]$db_host)) db_host <- secret_dict[[key]]$db_host
    if (db_name == "" && !is.null(secret_dict[[key]]$db_name)) db_name <- secret_dict[[key]]$db_name
    if (db_user == "" && !is.null(secret_dict[[key]]$db_user)) db_user <- secret_dict[[key]]$db_user
    if (db_pass == "" && !is.null(secret_dict[[key]]$db_pass)) db_pass <- secret_dict[[key]]$db_pass
  }
  if (db_host == "" || db_name == "") stop(str_c("Could not derive all required variables for database connectivity. Have db_host:", db_host, ", db_name:", db_name))
  return(dbConnect(
    drv = MariaDB(),
    host = db_host,
    dbname = db_name,
    user = db_user,
    password = db_pass,
    bigint = bigint,
    load_data_local_infile = TRUE,
    autocommit = TRUE,
    reconnect = TRUE,
    ...
  ))
}

#' List schemas in a database
#' @param con the connection to probe
#' @export
#' @importFrom dplyr tbl pull
#' @importFrom dbplyr in_schema
#' @return a list of the schemas defined in the database
list_schemas <- function(con) {
  tbl(con, dbplyr::in_schema("information_schema", "SCHEMATA")) %>%
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
#' @importFrom rlang .data
register_tables <- function(con, schemas, envir = .GlobalEnv) {
  d <- tbl(con, dbplyr::in_schema("information_schema", "TABLES")) %>%
    filter(.data$TABLE_SCHEMA %in% schemas) %>%
    select(.data$TABLE_SCHEMA, .data$TABLE_NAME) %>%
    collect()
  walk2(d$TABLE_NAME, d$TABLE_SCHEMA, ~ try(assign(.x, tbl(con, dbplyr::in_schema(.y, .x)), envir = envir)))
}

#' List all "temporary" tables starting with `tmp_` in the given schema
#' @param con the connection to probe
#' @param ... the schemas to probe
#' @export
#' @importFrom dplyr tbl filter select collect
#' @importFrom dbplyr in_schema
#' @importFrom stringr str_detect
#' @importFrom rlang .data
#' @return a tibble with TABLE_SCHEMA and TABLE_NAME columns containing information of the temporary tables found.
list_temporary_tables <- function(con, ...) {
  schemas <- c(...)
  tbl(con, dbplyr::in_schema("information_schema", "TABLES")) %>%
    filter(.data$TABLE_SCHEMA %in% schemas, str_detect(.data$TABLE_NAME, "^tmp_")) %>%
    select(.data$TABLE_SCHEMA, .data$TABLE_NAME) %>%
    collect()
}

#' Delete all "temporary" tables starting with `tmp_` in the given schemas
#' @param con the connection to probe
#' @param ... the schemas to probe
#' @export
#' @importFrom DBI dbRemoveTable Id
#' @importFrom purrr walk2
delete_temporary_tables <- function(con, ...) {
  d <- list_temporary_tables(con, ...)
  walk2(d$TABLE_SCHEMA, d$TABLE_NAME, ~ dbRemoveTable(con, Id(schema = .x, table = .y)))
}

#' Utility function to generate unique table names
#' @keywords internal
#' @importFrom stats runif
#' @return a unique table name
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
#' @param temporary whether to create a temporary table (defaults to TRUE if table name not specified, otherwise needs to be explicitly specified)
#' @param overwrite whether to overwrite existing tables (default to TRUE for temporary tables, FALSE otherwise)
#' @param ... Other arguments passed on to [dplyr::compute()],
#' @export
#' @importFrom dplyr filter compute
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
#' @return a dbplyr tbl referencing the table computed
compute_c <- function(sql, name, temporary, overwrite, ...) {
  if (missing(name)) {
    name <- unique_table_name()
    if (missing(temporary)) {
      temporary <- TRUE
    }
  }
  if (missing(temporary)) stop('argument "temporary" is missing, with no default')
  if (missing(overwrite)) overwrite <- temporary
  con <- sql %>% dbplyr::remote_con()
  if (overwrite) dbExecute(con, str_c("DROP TABLE IF EXISTS ", dbplyr::as.sql(name, con)))
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Columnstore")
  r <- sql %>%
    filter(0L == 1L) %>%
    compute(name = dbplyr::as.sql(name, con), temporary = FALSE, ...)
  dbExecute(con, str_c("INSERT INTO ", dbplyr::as.sql(name, con), " ", sql %>% dbplyr::remote_query()))
  dbExecute(con, str_c("SET SESSION storage_engine=", engine))
  if (temporary == TRUE) {
    fe <- new.env(parent = emptyenv())
    fe$con <- con
    fe$table_name <- r %>% dbplyr::remote_name()
    attr(r, "finalizer_env") <- fe
    reg.finalizer(fe, delete_table_finalizer, onexit = TRUE)
  }
  r
}

#' Version of [dplyr::compute()] that creates Aria tables.
#' @param sql the sql to compute
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param temporary whether to create a temporary table (defaults to TRUE if table name not specified, otherwise needs to be explicitly specified)
#' @param overwrite whether to overwrite existing tables (default to TRUE for temporary tables, FALSE otherwise)
#' @param ... Other arguments passed on to [dplyr::compute()],
#' @export
#' @importFrom dplyr compute
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
#' @return a dbplyr tbl referencing the table computed
compute_a <- function(sql, name, temporary, overwrite, ...) {
  if (missing(name)) {
    name <- unique_table_name()
    if (missing(temporary)) {
      temporary <- TRUE
    }
  }
  if (missing(temporary)) stop('argument "temporary" is missing, with no default')
  if (missing(overwrite)) overwrite <- temporary
  con <- sql %>% dbplyr::remote_con()
  if (overwrite) dbExecute(sql$src$con, str_c("DROP TABLE IF EXISTS ", dbplyr::as.sql(name, con)))
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Aria")
  r <- sql %>%
    compute(name = dbplyr::as.sql(name, con), temporary = FALSE, ...)
  dbExecute(con, str_c("SET SESSION storage_engine=", engine))
  if (temporary == TRUE) {
    fe <- new.env(parent = emptyenv())
    fe$con <- con
    fe$table_name <- r %>% dbplyr::remote_name()
    attr(r, "finalizer_env") <- fe
    reg.finalizer(fe, delete_table_finalizer, onexit = TRUE)
  }
  r
}

#' Version of [dplyr::copy_to()] that creates ColumnStore tables and has a better parameter order
#' @param df the dataframe to copy to the SQL store
#' @param con the connection to the SQL store
#' @param name the name of the table to create (defaults to a new unique table name)
#' @param temporary whether to create a temporary table (defaults to TRUE if table name not specified, otherwise needs to be explicitly specified)
#' @param overwrite whether to overwrite existing tables (default to TRUE for temporary tables, FALSE otherwise)
#' @param ... Other arguments passed on to [dplyr::copy_to()],
#' @export
#' @importFrom dplyr copy_to
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
#' @return a dbplyr tbl referencing the table created
copy_to_c <- function(df, con, name, temporary, overwrite, ...) {
  if (missing(name)) {
    name <- unique_table_name()
    if (missing(temporary)) {
      temporary <- TRUE
    }
  }
  if (missing(temporary)) stop('argument "temporary" is missing, with no default')
  if (missing(overwrite)) overwrite <- temporary
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Columnstore")
  r <- copy_to(con, df, name = name, overwrite = overwrite, temporary = FALSE, ...)
  dbExecute(con, str_c("SET SESSION storage_engine=", engine))
  if (temporary == TRUE) {
    fe <- new.env(parent = emptyenv())
    fe$con <- con
    fe$table_name <- r %>% dbplyr::remote_name()
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
#' @param overwrite whether to overwrite existing tables (default to TRUE for temporary tables, FALSE otherwise)
#' @param ... Other arguments passed on to [dplyr::copy_to()],
#' @export
#' @importFrom dplyr copy_to
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom stringr str_c
#' @return a dbplyr tbl referencing the table created
copy_to_a <- function(df, con, name, temporary, overwrite, ...) {
  if (missing(name)) {
    name <- unique_table_name()
    if (missing(temporary)) {
      temporary <- TRUE
    }
  }
  if (missing(temporary)) stop('argument "temporary" is missing, with no default')
  if (missing(overwrite)) overwrite <- temporary
  engine <- dbGetQuery(con, "SHOW SESSION VARIABLES LIKE 'storage_engine'")[[2]]
  dbExecute(con, "SET SESSION storage_engine=Aria")
  r <- copy_to(con, df, name = name, overwrite = overwrite, temporary = FALSE, ...)
  dbExecute(con, str_c("SET SESSION storage_engine=", engine))
  if (temporary == TRUE) {
    fe <- new.env(parent = emptyenv())
    fe$con <- con
    fe$table_name <- r %>% dbplyr::remote_name()
    attr(r, "finalizer_env") <- fe
    reg.finalizer(fe, delete_table_finalizer, onexit = TRUE)
  }
  r
}
