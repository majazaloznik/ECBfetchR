test_that("prepare vintage new", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    vintage_table <- prepare_vintage_table("ECS.Q.I9.N.4D1.CNS051_50.A1", con_test)
    expect_true(nrow(vintage_table) == 1)
    expect_true(all(colnames(vintage_table) == c("series_id", "published")))
  })
})


test_that("prepare vintage existing", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    vintage_table <- prepare_vintage_table("BSI.M.AT.N.A.T00.A.1.Z5.0000.Z01.E", con_test)
    expect_true(is.null(vintage_table))
  })
})






