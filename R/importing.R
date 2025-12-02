#' Import structure for a single ECB series (dimensions, levels, series etc)
#'
#' NB: we are not using categories for the ECB dataflows since the website doesn't
#' have a clean hierarchy.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con Database connection object
#' @param source_id Integer source ID, default 8 for ECB
#' @param schema defautls to "platform"
#' @param keep_vintage logical indicating whether to keep vintages, defaults to F
#'
#' @export
ECB_import_structure <- function(series_key, con,  source_id = 8, schema = "platform",
                                    keep_vintage = FALSE) {
  message("Importing structure data for ", series_key)

  # Create list to store all results
  insert_results <- list()

  # prepare and insert table if needed
  dataflow_code <- regmatches(series_key, regexpr("^[[:alnum:]]+", series_key))
  table_table <- prepare_table_table(dataflow_code, source_id, con, schema,
                                     keep_vintage)
  if(!is.null(table_table)){
  insert_results$table <- UMARimportR::insert_new_table_table(con, table_table, schema)
  message("Table insert: ", insert_results$table$count, " rows")} else {
    message("No table inserted.")}

  # prepare and insert table dimension table
  table_dimension_table <- prepare_table_dimensions_table(series_key, con, schema)
  if(!is.null(table_dimension_table)){
  insert_results$table_dimensions <- UMARimportR::insert_new_table_dimensions(
    con, table_dimension_table, schema)
  message("Table dimensions insert: ", insert_results$table_dimensions$count, " rows")} else {
    message("No table dimensions inserted.")}

  # prepare and select dimension levels before inserting them
  dimension_levels_table <- prepare_dimension_levels_table(series_key, con, schema)
  if(!is.null(dimension_levels_table)){
  insert_results$dimension_levels <- UMARimportR::insert_new_dimension_levels(
    con, dimension_levels_table, schema)
  message("Dimension levels insert: ", insert_results$dimension_levels$count, " rows")} else {
    message("No dimensions levels inserted.")}

  # prepare and insert series table
  series_table <- prepare_series_table(series_key, con, schema)
  if(!is.null(series_table)){
  insert_results$series <- UMARimportR::insert_new_series(con, series_table, schema)
  message("Series insert: ", insert_results$series$count, " rows")} else {
    message("No series inserted.")}

  # prepare and insert series levels table
  series_levels_table <- prepare_series_levels_table(series_key, con, schema)
  if(!is.null(series_levels_table)){
  insert_results$series_levels <- UMARimportR::insert_new_series_levels(
    con, series_levels_table, schema)
  message("Series levels insert: ", insert_results$series_levels$count, " rows")} else {
    message("No series levels inserted.")}

  invisible(insert_results)
}





#' Insert data points from ECB
#'
#' Function to prepare and insert ECB data points. The function first prepares
#' the required vintages and inserts them, then prepares the data points
#' table and inserts it. The function returns the results invisibly.
#'
#' This is a ECB specific function, which should be followed by the generic
#' UMARimportR function to write the vintage hashes and clean up redundant
#' vintages.
#'
#' @param series_key Character. An ECB series key (e.g., "ICP.M.U2.N.000000.4.ANR").
#' @param con Database connection
#' @param schema Schema name
#'
#' @return Insertion results (invisibly)
#' @export
ECB_import_data_points <- function(series_key, con, schema = "platform") {
  message("Preparing vintage for: ", series_key, " into schema ", schema)
  # collect outputs from the functions into one result list
  result <- list()
  # Try to prepare  vintage table but catch any errors
  vintage_result <- tryCatch(
    expr = {list(
      vintages = prepare_vintage_table(series_key, con, schema),
      error = NULL)},
    error = function(e) {
      error_msg <- conditionMessage(e)
      message("Note: ", error_msg)
      return(list(
        vintages = NULL,
        error = error_msg))})
  # Store error message if any
  result$vintage_error <- vintage_result$error
  # Only proceed with import if vintages were prepared successfully
  if (!is.null(vintage_result$vintages)) {
    # import vintages
    result$vintages <- UMARimportR::insert_new_vintage(con, vintage_result$vintages, schema)
    message("Vintage imported for: ", series_key)

    # Prepare data in ecb-specific way
    prep_data <- prepare_ecb_data_for_insert(series_key, con, schema)
    # Insert the prepared data
    result$data <- UMARimportR::insert_prepared_data_points(prep_data, con, schema)
  } else {
    message("Skipping import for ", series_key, " due to vintage preparation issue: ", vintage_result$error)
  }
  invisible(result)
}

