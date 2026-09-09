#!/usr/bin/env Rscript

pkgdown::build_site(new_process = FALSE, install = FALSE, preview = FALSE)

readme <- readLines("README.md", warn = FALSE, encoding = "UTF-8")
readme <- sub("# CanardAbsurd", "", readme, fixed = TRUE)
readme <- sub('src="man/figures/logo.png"',
  paste0('src="', normalizePath("man/figures/logo.png", winslash = "/"), '"'),
  readme, fixed = TRUE)
metadata <- c(
  "---",
  "title: CanardAbsurd",
  "output:",
  "  html:",
  "    options:",
  "      toc: true",
  "    meta:",
  paste0('      css: ["@default@1.14.69", "@article@1.14.69", "',
    normalizePath("tools/landing.css", winslash = "/"), '"]'),
  paste0('      include_before: "',
    normalizePath("tools/landing-header.html", winslash = "/"), '"'),
  "---"
)
litedown::mark(text = c(metadata, readme), output = "artifacts/pkgdown/index.html")
