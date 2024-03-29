% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/hscidbutil.R
\name{list_temporary_tables}
\alias{list_temporary_tables}
\title{List all "temporary" tables starting with \code{tmp_} in the given schema}
\usage{
list_temporary_tables(con, ...)
}
\arguments{
\item{con}{the connection to probe}

\item{...}{the schemas to probe}
}
\value{
a tibble with TABLE_SCHEMA and TABLE_NAME columns containing information of the temporary tables found.
}
\description{
List all "temporary" tables starting with \code{tmp_} in the given schema
}
