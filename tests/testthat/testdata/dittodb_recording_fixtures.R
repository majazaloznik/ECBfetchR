
source("tests/testthat/helper-connection.R")
con_test <- make_test_connection()


start_db_capturing()
con_test <- make_test_connection()
out <- prepare_source_table(con_test, schema = "platform")
UMARimportR::insert_new_source(con_test, out)
stop_db_capturing()

start_db_capturing()
con_test <- make_test_connection()
table_table <- ECBfetchR:::prepare_table_table("ICP", 8, con_test)
stop_db_capturing()

start_db_capturing()
con_test <- make_test_connection()
fix_ecb_url()
tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(con_test, "ICP")
dim_list <- ecb::get_dimensions("ICP.M.U2.N.000000.4.ANR")
table_dimensions <- ECBfetchR:::prepare_ecb_table_dimensions(tbl_id, dim_list, con_test)
UMARimportR::insert_new_table_dimensions(con_test, table_dimensions)
stop_db_capturing()



