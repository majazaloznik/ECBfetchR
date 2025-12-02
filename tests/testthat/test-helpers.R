test_that("construct_series_code works correctly", {

  fix_ecb_url()

  # Test with ICP series
  code1 <- construct_series_code("ICP.M.U2.N.000000.4.ANR")
  expect_equal(code1, "ECB--ICP--U2--N--000000--4--ANR--M")

  # Test with ECS series
  code2 <- construct_series_code("ECS.Q.I9.N.4D1.CNS051_50.A1")
  expect_equal(code2, "ECB--ECS--I9--N--4D1--CNS051_50--A1--Q")

  # Test with different frequency (annual)
  code3 <- construct_series_code("ICP.A.U2.N.000000.4.AVR")
  expect_equal(code3, "ECB--ICP--U2--N--000000--4--AVR--A")

  # Verify structure: should always have ECB at start and frequency at end
  expect_true(grepl("^ECB--", code1))
  expect_true(grepl("--M$", code1))
  expect_true(grepl("--Q$", code2))
  expect_true(grepl("--A$", code3))

  # Verify no FREQ dimension appears in the middle
  expect_false(grepl("--M--", code1))
  expect_false(grepl("--Q--", code2))
})

test_that("construct_series_code handles different dataflows", {

  fix_ecb_url()

  # Test short dataflow code
  code_short <- construct_series_code("BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E")
  expect_true(grepl("^ECB--BSI--", code_short))
  expect_true(grepl("--M$", code_short))
})


test_that("period format works ", {
  expect_true(format_period_id("2000", "A") =="2000")
  expect_true(format_period_id("2000-Q2", "Q") == "2000Q2")
  expect_true(format_period_id("2000-03", "M") == "2000M03")
})
