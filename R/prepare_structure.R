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
#'
#' @return Data frame with columns: code, name, source_id, url, notes, keep_vintage.
#'   Returns NULL if table already exists.
#' @export
prepare_table_table <- function(dataflow_code, source_id, con) {

  # Check if table exists
  existing_table <- DBI::dbGetQuery(
    con,
    "SELECT id FROM platform.table WHERE code = $1 AND source_id = $2",
    params = list(dataflow_code, source_id)
  )

  if (nrow(existing_table) > 0) {
    cat(sprintf("Table '%s' already exists (id: %d)\n", dataflow_code, as.numeric(existing_table$id)))
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
    notes = jsonlite::toJSON(list(dataflow = dataflow_code), auto_unbox = TRUE),
    keep_vintage = TRUE,
    stringsAsFactors = FALSE
  )
}

#' Prepare ECB table dimensions metadata
#'
#' Extracts unique key dimensions from ECB series (excluding attributes) and
#' checks which dimensions need to be inserted into the database. Only processes
#' dimensions that appear in the series keys, not metadata attributes like
#' COLLECTION, TITLE_COMPL, or UNIT.
#'
#' @param table_id Integer. The table_id from platform.table for which to
#'   prepare dimensions.
#' @param dims_list Named list. Output from \code{ecb::get_dimensions()} called
#'   on one or more ECB series keys. Each element contains a data frame with
#'   dimension names and values.
#' @param con Database connection object (e.g., from \code{DBI::dbConnect()}).
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
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(...)
#' dims_list <- ecb::get_dimensions("ICP.M.U2.N.000000.4.ANR")
#'
#' # Prepare dimensions for insertion
#' dims_to_insert <- prepare_ecb_table_dimensions(
#'   table_id = 15,
#'   dims_list = dims_list,
#'   con = con
#' )
#'
#' # Insert if needed
#' if (!is.null(dims_to_insert)) {
#'   # Insert logic here
#' }
#' }
#'
#'
#' @export
prepare_table_dimensions_table <- function(table_id, dims_list, con) {

  cat("\n--- Checking dimensions ---\n")

  # Extract key dimensions only from first series
  first_series_key <- names(dims_list)[1]
  key_dims <- extract_key_dimensions(first_series_key, dims_list[[1]])

  dim_names <- key_dims$dim

  # Check which dimensions already exist
  existing_dims <- DBI::dbGetQuery(
    con,
    "SELECT dimension FROM platform.table_dimensions WHERE table_id = $1",
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
