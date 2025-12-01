
source("tests/testthat/helper-connection.R")
con_test <- make_test_connection()


# start_db_capturing()
# con_test <- make_test_connection()
# out <- prepare_source_table(con_test, schema = "platform")
# UMARimportR::insert_new_source(con_test, out)
# stop_db_capturing()
#
start_db_capturing()
con_test <- make_test_connection()
table_table <- prepare_table_table("ICP", 8, con_test)
stop_db_capturing()

start_db_capturing()
con_test <- make_test_connection()
table_table <- prepare_table_table("AGR", 8, con_test)
stop_db_capturing()
#
# start_db_capturing()
# con_test <- make_test_connection()
# fix_ecb_url()
# table_dimensions <- ECBfetchR:::prepare_table_dimensions_table(
#   "ICO.A.AT.S128.L.W0.A.EUR",  con_test)
# stop_db_capturing()
#
# start_db_capturing()
# con_test <- make_test_connection()
# fix_ecb_url()
# dim_list <- ecb::get_dimensions("ICP.M.U2.N.000000.4.ANR")
# dimensions_levels <- ECBfetchR:::prepare_dimension_levels_table("ICP", dim_list, con_test)
# stop_db_capturing()

# con_test <- make_test_connection()
# ECBfetchR::fix_ecb_url()
# UMARimportR::delete_table(con_test, 406)
# tt <- prepare_table_table("ICP", 8, con_test)
# UMARimportR::insert_new_table_table(con_test, tt)
# td <- prepare_table_dimensions_table("ICP.M.U2.N.000000.4.ANR", con_test)
# UMARimportR::insert_new_table_dimensions(con_test, td)


# ============================================================================
# Record Series 1: ICP.M.U2.N.000000.4.ANR - levels DON'T exist
# ============================================================================

# Clear dimension levels for this specific series
table_id <- UMARaccessR::sql_get_table_id_from_table_code(con_test, "ICP", "platform")
tab_dims <- DBI::dbGetQuery(
  con_test,
  "SELECT id FROM platform.table_dimensions WHERE table_id = $1",
  params = list(table_id)
)

# Delete levels with values M, U2, N, 000000, 4, ANR
# Delete ALL dimension levels for ICP
for (tab_dim_id in tab_dims$id) {
  DBI::dbExecute(
    con_test,
    "DELETE FROM platform.dimension_levels WHERE tab_dim_id = $1",
    params = list(as.integer(tab_dim_id))
  )
}

cat("Recording fixtures for ICP.M.U2... (will prompt you)\n")
fix_ecb_url()
start_db_capturing()
con_test <- make_test_connection()
result1 <- ECBfetchR:::prepare_dimension_levels_table(
  series_key = "ICP.M.U2.N.000000.4.ANR",
  con = con_test
)
# Type: Monthly, Euro area, Not seasonally adjusted, Overall index, Eurostat, Annual rate of change
stop_db_capturing()

# Insert those levels
if (!is.null(result1)) {
UMARimportR::insert_new_dimension_levels(con_test, result1)
  cat("Inserted U2 levels\n")
}

# ============================================================================
# Record Series 2: ICP.M.DE.N.000000.4.ANR - levels DO exist
# ============================================================================
result2 <- result1[2,] |>
  dplyr::mutate(level_value = "DE", level_text = "Germany")
UMARimportR::insert_new_dimension_levels(con_test, result2)


start_db_capturing()
con_test <- make_test_connection()
result2 <- ECBfetchR:::prepare_dimension_levels_table(
  series_key = "ICP.M.DE.N.000000.4.ANR",
  con = con_test)
stop_db_capturing()

start_db_capturing()
con_test <- make_test_connection()
fix_ecb_url()
series_table <- prepare_series_table("ICP.M.DE.N.000000.4.ANR", con_test, schema = "platform")
stop_db_capturing()


start_db_capturing()
con_test <- make_test_connection()
fix_ecb_url()
# result1 <- ECBfetchR:::prepare_dimension_levels_table(
#   series_key = "ICP.M.SI.N.000000.4.ANR",
#   con = con_test)
# UMARimportR::insert_new_dimension_levels(con_test, result1)
series_table <- prepare_series_table("ICP.M.SI.N.000000.4.ANR", con_test, schema = "platform")
# UMARimportR::insert_new_series(con_test, series_table)
stop_db_capturing()

