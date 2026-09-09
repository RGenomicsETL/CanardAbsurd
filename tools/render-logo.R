#!/usr/bin/env Rscript

# TeX Gyre Pagella and TeX Gyre Chorus supply the source lettering.
# Cairo exports the lettering as paths so the distributable SVG needs no fonts.
dir.create("man/figures", recursive = TRUE, showWarnings = FALSE)
rsvg::rsvg_svg("tools/logo.svg", "man/figures/logo.svg")
rsvg::rsvg_png("man/figures/logo.svg", "man/figures/logo.png", width = 1200)
