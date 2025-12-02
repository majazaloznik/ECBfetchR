test_that("import structure", {
  local_mocked_bindings(
    readline = function(prompt = "") {
      return("")
    },
    .package = "base"
  )
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    out <- ECB_import_structure("ICP.M.HU.N.XEF000.4.ANR", con_test)
    expect_true(is.list(out))
    print(out)
    expect_true(length(out) == 1)
  })
})

test_that("import structure", {
  local_mocked_bindings(
    readline = function(prompt = "") {
      return("")
    },
    .package = "base"
  )
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    out <- ECB_import_structure("BSI.M.AT.N.A.T00.A.1.Z5.0000.Z01.E", con_test)
    expect_true(is.list(out))
    expect_true(length(out) == 1)
  })
})

test_that("import structure", {
  local_mocked_bindings(
    readline = function(prompt = "") {
      return("")
    },
    .package = "base"
  )
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    out <- ECB_import_structure("ECS.Q.I9.N.4D1.CNS051_50.A1", con_test)
    expect_true(is.list(out))
    print(out)
    expect_true(length(out) == 4)
  })
})

test_that("prepare vintage existing", {
  dittodb::with_mock_db({
    con_test <- make_test_connection()
    fix_ecb_url()
    result <- ECB_import_data_points("ECS.Q.I9.N.4D1.CNS051_50.A1", con_test)
    expect_true(is.list(result))
    expect_true(result$data$datapoints_inserted == 87)
  })
})


