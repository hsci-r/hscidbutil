test_that("copy_to_c works", {
  con <- mariadbDefault()
  on.exit(dbDisconnect(con))

  x <- data.frame(col1 = 1:10, col2 = letters[1:10])

  d <- copy_to_c(x,con)

  dl <- collect(d)

  expect_equal(dl$col1,x$col1)
  expect_equal(dl$col2,x$col2)

  delete_temporary_tables(con)
})

test_that("copy_to_a works", {
  con <- mariadbDefault()
  on.exit(dbDisconnect(con))

  x <- data.frame(col1 = 1:10, col2 = letters[1:10])

  d <- copy_to_a(x,con)

  dl <- collect(d)

  expect_equal(dl$col1,x$col1)
  expect_equal(dl$col2,x$col2)
})
