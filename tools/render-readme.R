#!/usr/bin/env Rscript

rmarkdown::render("README.Rmd", quiet = TRUE,
  envir = new.env(parent = globalenv()), intermediates_dir = tempdir())
markdown <- readLines("README.md", warn = FALSE, encoding = "UTF-8")
writeLines(sub("[[:blank:]]+$", "", markdown), "README.md", useBytes = TRUE)
