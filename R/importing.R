#' Import structure for a single ECB series (dimensions, levels, series etc)
#'
#' NB: we are not using categories for the ECB dataflows since the website doesn't
#' have a clean hierarchy.
#'
#' @param series_key Character, ecb dataset code
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
