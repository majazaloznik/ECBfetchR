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

test_that("prepare table table", {
  local_mocked_bindings(
    readline = function(prompt = "") {
      return("")
    },
    .package = "base"
  )

  dittodb::with_mock_db({
    con_test <- make_test_connection()
    out <- prepare_table_table("AGR", 8, con_test)
    expect_true(all(dim(out) == c(1,6)))
  })
})

test_that("prepare table dimensions table", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    table_dimensions <- prepare_table_dimensions_table("ICO.A.AT.S128.L.W0.A.EUR", con_test)
    expect_equal(dim(table_dimensions), c(7,3))
  })
})

test_that("prepare_dimension_levels_table works for new dimension levels", {

  mock_readline_values <- c(
    "Monthly", "Euro area", "Not seasonally adjusted",
    "Overall index", "Eurostat", "Annual rate of change"
  )
  mock_readline_counter <- 1

  local_mocked_bindings(
    readline = function(prompt = "") {
      value <- mock_readline_values[mock_readline_counter]
      mock_readline_counter <<- mock_readline_counter + 1
      return(value)
    },
    .package = "base"
  )

  with_mock_db({
    con <- make_test_connection()

    result <- prepare_dimension_levels_table(
      series_key = "ICP.M.U2.N.000000.4.ANR",  # U2 series
      con = con
    )

    expect_s3_class(result, "data.frame")
    expect_named(result, c("tab_dim_id", "level_value", "level_text"))
    expect_equal(nrow(result), 6)
    expect_true("U2" %in% result$level_value)
    expect_true("Euro area" %in% result$level_text)
  })
})

test_that("prepare_dimension_levels_table returns NULL when all levels exist", {

  local_mocked_bindings(
    readline = function(prompt = "") {
      stop("readline should not be called when all levels exist")
    },
    .package = "base"
  )

  with_mock_db({
    con <- make_test_connection()

    result <- prepare_dimension_levels_table(
      series_key = "ICP.M.DE.N.000000.4.ANR",  # DE series - levels exist
      con = con
    )

    expect_null(result)
  })
})

test_that("prepare_dimension_levels_table uses code as default when user enters blank", {

  local_mocked_bindings(
    readline = function(prompt = "") {
      return("")
    },
    .package = "base"
  )

  with_mock_db({
    con <- make_test_connection()

    result <- prepare_dimension_levels_table(
      series_key = "ICP.M.U2.N.000000.4.ANR",  # U2 series
      con = con
    )

    expect_s3_class(result, "data.frame")
    expect_equal(result$level_value, result$level_text)
  })
})

test_that("prepare_series_table", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    series_table <- prepare_series_table("ICP.M.DE.N.000000.4.ANR", con_test, schema = "platform")
    expect_s3_class(series_table, "data.frame")
    expect_true(all(dim(series_table) == c(1,5)))
    series_table <- prepare_series_table("ICP.M.SI.N.000000.4.ANR", con_test, schema = "platform")
    expect_true(is.null(series_table))
  })
})

test_that("prepare_series_levels_table", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    series_levels <- prepare_series_levels_table("ICP.M.SI.N.000000.4.ANR", con_test, schema = "platform")
    expect_s3_class(series_levels, "data.frame")
    expect_true(all(dim(series_levels) == c(6,3)))
    series_levels <- prepare_series_levels_table("ICP.M.GR.N.000000.4.ANR", con_test, schema = "platform")
    expect_true(is.null(series_levels))
  })
})





