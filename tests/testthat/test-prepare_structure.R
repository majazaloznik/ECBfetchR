test_that("prepare source table", {
  dittodb::with_mock_db({
    con <- make_connection()
    out <- prepare_source_table(con, schema = "platform")
    expect_equal(nrow(out), 1)
    expect_equal(out$id, 8)
  })
})

test_that("prepare table table", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    out <- prepare_table_table("ICP", 8, con_test)
    expect_true(is.null(out))
  })
})

test_that("prepare table dimensions table", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    tbl_id <- UMARaccessR::sql_get_table_id_from_table_code(con_test, "ICP")
    dim_list <- ecb::get_dimensions("ICP.M.U2.N.000000.4.ANR")
    table_dimensions <- ECBfetchR:::prepare_table_dimensions_table(tbl_id, dim_list, con_test)
    expect_equal(dim(table_dimensions), c(6,3))
  })
})
