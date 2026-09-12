#!/usr/bin/env Rscript

knitr::opts_chunk$set(eval = TRUE, cache = FALSE, collapse = TRUE,
                    comment = "#>", error = FALSE, message = FALSE)
rmarkdown::render("README.Rmd", quiet = TRUE,
  envir = new.env(parent = globalenv()), intermediates_dir = tempdir())
markdown <- readLines("README.md", warn = FALSE, encoding = "UTF-8")
writeLines(sub("[[:blank:]]+$", "", markdown), "README.md", useBytes = TRUE)
