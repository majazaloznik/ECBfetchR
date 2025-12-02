#' Prepare table to insert into `vintage` table
#'
#' Helper function that populates the vintage table with the new vintages. It gets
#' the series id's from the database and adds the publication date from the px.
#'
#' Returns table ready to insert into the `vintage`table with the
#' UMARimportr::insert family of functions.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con a connection to the database
#' @param schema the schema to use for the connection, default is "platform"
#'
#' @return a dataframe with the `series_id` and `published` columns
#' for all the series in this table.
#' @export
prepare_vintage_table <- function(series_key, con, schema = "platform") {

  series_code <- construct_series_code(series_key)
  series_id_result <- UMARaccessR::sql_get_series_id_from_series_code(series_code, con)

  # Handle atomic NA
  if (length(series_id_result) == 1 && is.na(series_id_result)) {
    stop("Series '", series_code, "' not found in database")
  }

  # Handle NULL
  if (is.null(series_id_result)) {
    stop("Series '", series_code, "' not found in database")
  }

  # Handle data frame
  if (is.data.frame(series_id_result)) {
    if (nrow(series_id_result) == 0) {
      stop("Series '", series_code, "' not found in database")
    }
    series_id <- as.numeric(series_id_result$id[1])
  } else {
    series_id <- as.numeric(series_id_result)
  }

  # Check if NA after conversion
  if (is.na(series_id)) {
    stop("Series '", series_code, "' not found in database")
  }

  # Get last modified from ECB API
  dataflow <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))
  key_part <- sub("^[[:alnum:]]+\\.", "", series_key)

  resp <- httr::GET(
    sprintf("https://data-api.ecb.europa.eu/service/data/%s/%s", dataflow, key_part),
    query = list(detail = "dataonly")
  )

  last_modified <- httr::parse_http_date(httr::headers(resp)$`last-modified`)
  last_modified_utc <- lubridate::force_tz(last_modified, "UTC")

    # Compare to our last import
  vin_id <- UMARaccessR::sql_get_vintage_from_series_code(con, series_code, NULL, schema)
  latest <- UMARaccessR::sql_get_date_published_from_vintage(vin_id, con, schema)
  if(!is.null(latest)){latest_utc <- lubridate::force_tz(latest, "UTC")}

  if (!is.null(latest) && latest_utc >= last_modified_utc) {
    cat("No updates since", format(latest, "%Y-%m-%d %H:%M:%S"), "no new vintages will be inserted.\n")
    return(NULL)
  }

  data.frame(series_id = series_id, published = last_modified)
}






#' Prepare ECB data  for insertion
#'
#' Processes raw ECB data into a format ready for database insertion - this
#' is a single series funciton as opposed to most other fetchR prep_data functions.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con Database connection
#' @param schema the schema to use for the connection, default is "platform"
#'
#' @return A list containing:
#'  - data: The processed data frame. which has to have columns time and value,
#'  with the rest corresponding to the dimensions of the table/series
#'  - table_id: The table
#'  - interval_id: the interval id ("M", "A", or "Q")
#'  - dimension_ids: The non-time dimension IDs
#'  - dimension_names: The names of the dimensions
#' @export
prepare_ecb_data_for_insert <- function(series_key, con, schema = "platform") {
  # Get raw data
  table_code <- sub("\\.(.*)", "", series_key)
  tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(con, table_code, schema)
  raw <- ecb::get_data(series_key)
  interval_id <- unique(raw$freq)
  # prep dataframe, including adding dummy flag column..
  df <- raw |>
    dplyr::mutate(time = format_period_id(obstime, interval_id),
                  value = obsvalue,
                  flag = "",
                  .keep = "unused")
  # Assuming your data frame is called df
  names(df)[1:(ncol(df) - 3)] <- toupper(names(df)[1:(ncol(df) - 3)])
  # Get metadata
  dim_ids <- UMARaccessR::sql_get_non_time_dimensions_from_table_id(tbl_id, con, schema)

  # Return structured result
  list(
    data = df,
    table_id = tbl_id,
    interval_id = interval_id,
    dimension_ids = dim_ids$id,
    dimension_names = dim_ids$dimension
  )
}
