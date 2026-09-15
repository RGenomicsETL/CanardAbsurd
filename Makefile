R ?= R
RSCRIPT ?= Rscript

.PHONY: document install test build check quack logo readme site docs

document:
	$(RSCRIPT) --vanilla -e 'roxygen2::roxygenise()'

install:
	mkdir -p artifacts/library
	$(R) CMD INSTALL --library=artifacts/library .

quack:
	$(RSCRIPT) --vanilla tools/install-extensions.R

logo:
	R_LIBS="$(CURDIR)/artifacts/library$${R_LIBS:+:$$R_LIBS}" $(RSCRIPT) --vanilla tools/render-logo.R

readme: install
	R_LIBS="$(CURDIR)/artifacts/library$${R_LIBS:+:$$R_LIBS}" $(RSCRIPT) --vanilla tools/render-readme.R

site: install
	R_LIBS="$(CURDIR)/artifacts/library$${R_LIBS:+:$$R_LIBS}" $(RSCRIPT) --vanilla tools/build-site.R

docs: readme site

test: install
	R_LIBS="$(CURDIR)/artifacts/library$${R_LIBS:+:$$R_LIBS}" CANARDABSURD_REQUIRE_QUACK=true $(RSCRIPT) --vanilla -e 'library(CanardAbsurd); tinytest::test_package("CanardAbsurd")'

build:
	mkdir -p artifacts/source/CanardAbsurd
	rsync -a --delete --exclude=/artifacts --exclude=/.git --exclude=/.pi ./ artifacts/source/CanardAbsurd/
	cd artifacts && $(R) CMD build source/CanardAbsurd

check: build
	cd artifacts && CANARDABSURD_REQUIRE_QUACK=true $(R) CMD check --no-manual CanardAbsurd_*.tar.gz
