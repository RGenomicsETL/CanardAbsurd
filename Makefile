R ?= R
RSCRIPT ?= Rscript

.PHONY: document install test check quack readme site docs

document:
	$(RSCRIPT) --vanilla -e 'roxygen2::roxygenise()'

install:
	mkdir -p artifacts/library
	$(R) CMD INSTALL --library=artifacts/library .

quack:
	$(RSCRIPT) --vanilla tools/install-extensions.R

readme: install
	R_LIBS="$(CURDIR)/artifacts/library" $(RSCRIPT) --vanilla tools/render-readme.R

site: install
	R_LIBS="$(CURDIR)/artifacts/library" $(RSCRIPT) --vanilla tools/build-site.R

docs: readme site

test: install
	R_LIBS="$(CURDIR)/artifacts/library" CANARDABSURD_REQUIRE_QUACK=true $(RSCRIPT) --vanilla -e 'library(CanardAbsurd); tinytest::test_package("CanardAbsurd")'

check:
	mkdir -p artifacts
	cd artifacts && $(R) CMD build ..
	cd artifacts && CANARDABSURD_REQUIRE_QUACK=true $(R) CMD check --no-manual CanardAbsurd_*.tar.gz
