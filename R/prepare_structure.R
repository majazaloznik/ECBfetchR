#' Prepare table to insert into `source` table

#' Helper function that manually prepares the new line for the source table.
#'
#' @param con connection to the database.
#' @param schema the schema to use for the connection, default is "platform"
#' @return a dataframe with the `name`, `name_long` and `url` columns.
#' for this table.
#' @export
prepare_source_table <- function(con, schema = "platform") {
  DBI::dbExecute(con, paste0("set search_path to ", schema))
  source_id <- UMARaccessR::sql_get_source_code_from_source_name(con, "Eurostat", schema)
  if (is.null(source_id)){
    id <- dplyr::tbl(con, "source") |>
      dplyr::summarise(max = max(id, na.rm = TRUE)) |>
      dplyr::pull() + 1
    data.frame(id = id,
               name = "ECB",
               name_long = "European Central Bank",
               url = "https://data.ecb.europa.eu/data/datasets")} else {
                 message("ECB already listed in the source table.")}
}

#' Prepare ECB table metadata
#'
#' Checks if table exists and prepares data frame for insertion if needed.
#' Prompts user for descriptive table name for new dataflows.
#'
#' @param dataflow_code Character, ECB dataflow code (e.g., "BSI")
#' @param source_id Integer, source_id for ECB
#' @param con Database connection
#' @param schema defaults to platform
#' @param keep_vintage logical defaults to FALSE
#'
#' @return Data frame with columns: code, name, source_id, url, notes, keep_vintage.
#'   Returns NULL if table already exists.
#' @export
prepare_table_table <- function(dataflow_code, source_id, con, schema = "platform",
                                keep_vintage = FALSE) {

  # Check if table exists
  existing_table <- UMARaccessR::sql_get_table_id_from_table_code(con, dataflow_code)

  if (!is.na(existing_table)) {
    cat(sprintf("Table '%s' already exists (id: %d)\n", dataflow_code, as.numeric(existing_table)))
    return(NULL)
  }

  # New table - prompt for name
  cat(sprintf("\nNew ECB dataflow detected: %s\n", dataflow_code))
  table_name <- readline(prompt = "Enter descriptive table name (or press Enter to use code): ")
  if (table_name == "") table_name <- dataflow_code

  data.frame(
    code = dataflow_code,
    name = table_name,
    source_id = source_id,
    url = sprintf("https://data.ecb.europa.eu/data/datasets/%s", dataflow_code),
    notes = as.character(jsonlite::toJSON(list(), auto_unbox = TRUE)),
    keep_vintage = keep_vintage
  )
}

#' Prepare ECB table dimensions metadata
#'
#' Extracts unique key dimensions from ECB series (excluding attributes) and
#' checks which dimensions need to be inserted into the database. Only processes
#' dimensions that appear in the series keys, not metadata attributes like
#' COLLECTION, TITLE_COMPL, or UNIT.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con Database connection object (e.g., from \code{DBI::dbConnect()}).
#' @param schema Database schema defaults to platform
#'
#' @return A data frame with columns \code{table_id}, \code{dimension}, and
#'   \code{is_time} for dimensions that need to be inserted. Returns \code{NULL}
#'   if all dimensions already exist in the database.
#'
#' @details
#' The function determines which dimensions are "key dimensions" by counting the
#' number of dot-separated values in the series key (after the dataflow code).
#' For example, in the key "ICP.M.U2.N.000000.4.ANR", there are 6 dimension
#' values, so the first 6 dimensions from \code{get_dimensions()} output are
#' considered key dimensions. The remaining dimensions are attributes and are
#' excluded.
#'
#' The function queries the database to identify which dimensions already exist
#' for the given table and only returns new dimensions that need to be inserted.
#'
#' @export
prepare_table_dimensions_table <- function(series_key, con, schema = "platform") {

  dims_list <- ecb::get_dimensions(series_key)
  dataflow_code <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))
  table_id <- UMARaccessR::sql_get_table_id_from_table_code(con, dataflow_code, schema)

  cat("\n--- Checking dimensions ---\n")

  # Extract key dimensions only from first series
  first_series_key <- names(dims_list)[1]
  key_dims <- extract_key_dimensions(first_series_key, dims_list[[1]])

  dim_names <- key_dims$dim

  # Check which dimensions already exist
  existing_dims <- DBI::dbGetQuery(
    con,
    sprintf("/* Params: table_id=%s */
           SELECT dimension FROM platform.table_dimensions WHERE table_id = $1",
            table_id),
    params = list(table_id)
  )

  # Filter to new dimensions only
  new_dim_names <- setdiff(dim_names, existing_dims$dimension)

  if (length(new_dim_names) == 0) {
    cat("All dimensions already exist\n")
    return(NULL)
  }

  # Report status
  for (dim in dim_names) {
    if (dim %in% existing_dims$dimension) {
      cat(sprintf("  Dimension '%s' exists\n", dim))
    } else {
      cat(sprintf("  Dimension '%s' is new\n", dim))
    }
  }

  # Prepare data frame for new dimensions only
  data.frame(
    table_id = table_id,
    dimension = new_dim_names,
    is_time = FALSE,
    stringsAsFactors = FALSE
  )
}

#' Prepare ECB dimension levels metadata
#'
#' Extracts unique dimension level values from an ECB series key and prompts
#' the user for descriptions of new levels that don't exist in the database.
#' Only processes key dimensions (not attributes) by parsing the series key
#' structure.
#'
#' @param series_key Character. An ECB series key (e.g.,
#'   "ICP.M.U2.N.000000.4.ANR"). The function extracts the dataflow code from
#'   the first part of the key and uses the number of dot-separated values to
#'   determine which dimensions are key dimensions versus attributes.
#' @param con Database connection object (e.g., from \code{DBI::dbConnect()}).
#' @param schema Character. The database schema name. Default is "platform".
#'
#' @return A data frame with columns \code{tab_dim_id}, \code{level_value},
#'   and \code{level_text} for dimension levels that need to be inserted.
#'   Returns \code{NULL} if all dimension levels already exist in the database.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Calls \code{ecb::get_dimensions()} to retrieve dimension information
#'   \item Extracts the dataflow code and looks up the corresponding table_id
#'   \item Identifies which dimension levels already exist in the database
#'   \item For each new dimension level, prompts the user interactively to
#'         enter a description. If the user presses Enter without typing, the
#'         level code is used as the description.
#' }
#'
#' The function assumes that table dimensions have already been created via
#' \code{\link{prepare_table_dimensions_table}}.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#'
#' # Prepare dimension levels (will prompt for descriptions)
#' levels_to_insert <- prepare_dimension_levels_table(
#'   series_key = "ICP.M.U2.N.000000.4.ANR",
#'   con = con
#' )
#'
#' # Insert if needed
#' if (!is.null(levels_to_insert)) {
#'   # Insert logic here
#' }
#' }
#'
#'
#' @export
prepare_dimension_levels_table <- function(series_key, con, schema = "platform") {

  dims_list <- ecb::get_dimensions(series_key)
  dataflow_code <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))
  table_id <- UMARaccessR::sql_get_table_id_from_table_code(con, dataflow_code, schema)

  cat("\n--- Checking dimension levels ---\n")

  # Get tab_dim_ids for this table
  table_dims <- DBI::dbGetQuery(
    con,
    sprintf("/* Params: table_id=%s */
           SELECT id, dimension FROM platform.table_dimensions WHERE table_id = $1",
            table_id),
    params = list(table_id)
  )

  if (nrow(table_dims) == 0) {
    stop("No table dimensions found. Run prepare_table_dimensions_table first.")
  }

  # Create lookup: dimension name -> tab_dim_id
  dim_lookup <- setNames(table_dims$id, table_dims$dimension)

  # Extract all unique dimension-value pairs across all series (key dimensions only)
  all_dim_values <- purrr::map_dfr(names(dims_list), function(key) {
    key_dims <- extract_key_dimensions(key, dims_list[[key]])
    key_dims |>
      dplyr::select(dim, value)
  }) |>
    dplyr::distinct()

  # Add tab_dim_ids
  all_dim_values$tab_dim_id <- dim_lookup[all_dim_values$dim]

  # Check existing levels in database
  # Build IN clause with placeholders
  tab_dim_id_list <- as.integer(unname(dim_lookup))
  placeholders <- paste0("$", seq_along(tab_dim_id_list), collapse = ", ")

  query <- sprintf(
    "SELECT tab_dim_id, level_value
   FROM platform.dimension_levels
   WHERE tab_dim_id IN (%s)
    -- series: %s",
    placeholders,
    series_key
  )

  existing_levels <- DBI::dbGetQuery(con, query, params = as.list(tab_dim_id_list))

  # Create key for matching
  all_dim_values$key <- paste(all_dim_values$tab_dim_id, all_dim_values$value, sep = "_")
  existing_levels$key <- paste(existing_levels$tab_dim_id, existing_levels$level_value, sep = "_")

  # Filter to new levels only
  new_levels <- all_dim_values |>
    dplyr::filter(!key %in% existing_levels$key) |>
    dplyr::select(tab_dim_id, dimension = dim, level_value = value)

  if (nrow(new_levels) == 0) {
    cat("All dimension levels already exist\n")
    return(NULL)
  }

  # Report existing levels
  existing_count <- nrow(all_dim_values) - nrow(new_levels)
  if (existing_count > 0) {
    cat(sprintf("  %d dimension levels already exist\n", existing_count))
  }

  # Prompt for new level descriptions
  new_levels$level_text <- NA_character_

  for (i in seq_len(nrow(new_levels))) {
    cat(sprintf("\n  New dimension level:\n"))
    cat(sprintf("    Dataflow: %s\n", dataflow_code))
    cat(sprintf("    Dimension: %s\n", new_levels$dimension[i]))
    cat(sprintf("    Code: %s\n", new_levels$level_value[i]))

    level_text <- readline(prompt = "    Enter description, otherwise the code will be used: \n check at https://data.ecb.europa.eu/data/datasets/XXX/structure")
    if (level_text == "") {
      cat("    Using code as description\n")
      level_text <- new_levels$level_value[i]
    }

    new_levels$level_text[i] <- level_text
  }

  # Return only required columns
  new_levels |>
    dplyr::select(tab_dim_id, level_value, level_text)
}


#' Prepare ECB series metadata
#'
#' Prepares series metadata for insertion into the database. Constructs the
#' series code from source, dataflow, dimension values, and interval, and
#' builds the series title from dimension level descriptions.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con Database connection object.
#' @param schema Character. The database schema name. Default is "platform".
#'
#' @return A data frame with columns \code{series_title}, \code{series_code},
#'   \code{unit_id}, \code{table_id}, and \code{interval_id}. Returns \code{NULL}
#'   if the series already exists in the database.
#'
#' @details
#' The series code is constructed as:
#' \code{source_name--dataflow_code--dimension_values--interval}
#'
#' For example, "ICP.M.U2.N.000000.4.ANR" becomes:
#' "ECB--ICP--U2--N--000000--4--ANR--M"
#'
#' The series title is constructed by combining dimension level descriptions
#' from the database, separated by " -- ". For example:
#' "Euro area -- Not seasonally adjusted -- Overall index -- Eurostat -- Annual rate of change"
#'
#' The function assumes that table dimensions and dimension levels have already
#' been created via \code{\link{prepare_table_dimensions_table}} and
#' \code{\link{prepare_dimension_levels_table}}.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#'
#' series_df <- prepare_series_table(
#'   series_key = "ICP.M.U2.N.000000.4.ANR",
#'   source_name = "ECB",
#'   con = con
#' )
#' }
#'
#' @seealso
#' \code{\link{prepare_table_table}}, \code{\link{prepare_dimension_levels_table}}
#'
#' @export
prepare_series_table <- function(series_key, con, schema = "platform") {

  # Parse series key
  dims_list <- ecb::get_dimensions(series_key)
  key_dims <- extract_key_dimensions(series_key, dims_list[[1]])

  dataflow_code <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))

  # Get table_id
  table_id <- UMARaccessR::sql_get_table_id_from_table_code(con, dataflow_code, schema)

  # Extract interval (FREQ dimension)
  interval_id <- key_dims$value[key_dims$dim == "FREQ"]

  # Get dimension values excluding FREQ
  non_freq_dims <- key_dims[key_dims$dim != "FREQ", ]

  # Construct series_code: source--dataflow--dimension_values--interval
  series_code <- construct_series_code(series_key)

  # Check if series already exists
  existing_series <- DBI::dbGetQuery(
    con,
    sprintf("/* Params: code=%s, table_id=%s */
           SELECT id FROM %s.series WHERE code = $1 AND table_id = $2",
            series_code, table_id, schema),
    params = list(series_code, table_id)
  )

  if (nrow(existing_series) > 0) {
    cat(sprintf("Series '%s' already exists \n", series_code))
    return(NULL)
  }

  # Get table_dimensions for this table
  table_dims <- DBI::dbGetQuery(
    con,
    sprintf("/* Params: table_id=%s */
           SELECT id, dimension FROM %s.table_dimensions WHERE table_id = $1",
            table_id, schema),
    params = list(table_id)
  )

  # Create lookup: dimension name -> tab_dim_id
  dim_lookup <- setNames(table_dims$id, table_dims$dimension)

  # Get dimension level texts from database (excluding FREQ)
  level_texts <- character(nrow(non_freq_dims))

  for (i in seq_len(nrow(non_freq_dims))) {
    dim_name <- non_freq_dims$dim[i]
    level_value <- non_freq_dims$value[i]
    tab_dim_id <- dim_lookup[dim_name]

    level_info <- DBI::dbGetQuery(
      con,
      sprintf("/* Params: tab_dim_id=%s, level_value=%s */
           SELECT level_text FROM %s.dimension_levels
           WHERE tab_dim_id = $1 AND level_value = $2",
              tab_dim_id, level_value, schema),
      params = list(as.integer(tab_dim_id), level_value)
    )

    if (nrow(level_info) > 0) {
      level_texts[i] <- level_info$level_text
    } else {
      warning(sprintf("Level text not found for %s=%s", dim_name, level_value))
      level_texts[i] <- level_value  # Fallback to code
    }
  }

  # Construct series_title
  series_title <- paste(level_texts, collapse = " -- ")

  cat(sprintf("Prepared series: %s\n", series_code))

  # Return data frame
  data.frame(
    name_long = series_title,
    code = series_code,
    unit_id = NA_integer_,
    table_id = table_id,
    interval_id = interval_id,
    stringsAsFactors = FALSE
  )
}

#' Prepare ECB series levels metadata
#'
#' Prepares series level mappings for insertion into the database. Maps each
#' dimension value from the series key to its corresponding table dimension ID.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con Database connection object.
#' @param schema Character. The database schema name. Default is "platform".
#'
#' @return A data frame with columns \code{series_id}, \code{tab_dim_id}, and
#'   \code{level_value}. Returns \code{NULL} if the series doesn't exist in the
#'   database yet.
#'
#' @details
#' The function extracts dimension values from the series key and matches them
#' with table dimension IDs from the database. The frequency dimension (FREQ)
#' is moved from the first position to the last position to match the database
#' table_dimensions order.
#'
#' For example, key "ICP.M.U2.N.000000.4.ANR" with dimensions:
#' FREQ=M, REF_AREA=U2, ADJUSTMENT=N, ICP_ITEM=000000, STS_INSTITUTION=4, ICP_SUFFIX=ANR
#'
#' Is reordered to match table_dimensions:
#' REF_AREA=U2, ADJUSTMENT=N, ICP_ITEM=000000, STS_INSTITUTION=4, ICP_SUFFIX=ANR, FREQ=M
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#'
#' series_levels <- prepare_series_levels_table(
#'   series_key = "ICP.M.U2.N.000000.4.ANR",
#'   con = con
#' )
#' }
#'
#' @seealso
#' \code{\link{prepare_series_table}}
#'
#' @export
prepare_series_levels_table <- function(series_key, con, schema = "platform") {

  # Parse series key
  dims_list <- ecb::get_dimensions(series_key)
  key_dims <- extract_key_dimensions(series_key, dims_list[[1]])

  dataflow_code <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))

  # Get table_id
  table_id <- UMARaccessR::sql_get_table_id_from_table_code(con, dataflow_code, schema)

  series_code <- construct_series_code(series_key)

  # Get series_id
  series_id_result <- UMARaccessR::sql_get_series_id_from_series_code(series_code, con)

  # Handle atomic NA
  if (!is.data.frame(series_id_result) && length(series_id_result) == 1 && is.na(series_id_result)) {
    cat(sprintf("Series '%s' not found in database\n", series_code))
    return(NULL)
  }

  # Handle NULL or empty data frame
  if (is.null(series_id_result) || (is.data.frame(series_id_result) && nrow(series_id_result) == 0)) {
    cat(sprintf("Series '%s' not found in database\n", series_code))
    return(NULL)
  }

  # Extract and convert id
  if (is.data.frame(series_id_result)) {
    series_id <- as.numeric(series_id_result$id[1])
  } else {
    series_id <- as.numeric(series_id_result)
  }

  # Check if NA after conversion
  if (is.na(series_id)) {
    cat(sprintf("Series '%s' not found in database\n", series_code))
    return(NULL)
  }
  # Get table dimensions
  table_dims <- UMARaccessR::sql_get_dimensions_from_table_id(table_id, con, schema)
  # Join key_dims with table_dims to get tab_dim_ids
  series_levels <- key_dims |>
    dplyr::left_join(
      table_dims |> dplyr::select(dimension, tab_dim_id = id),
      by = c("dim" = "dimension")) |>
    dplyr::transmute(series_id = series_id,
                     tab_dim_id = tab_dim_id,
                     level_value = value)

  cat(sprintf("Prepared %d series levels for series_id %d\n",
              nrow(series_levels), series_id))

  series_levels
}
