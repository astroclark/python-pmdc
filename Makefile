# Use this Makefile as a template for 
#   debian "native" packages
# 
# Customize it by modifying 2 variables: PKGNAME and VERSION
# 
# 'make chk_ver' will run checks on debian package version and 
# SL package version.  It will error out if the versions do not
# agree.
#
# 
# 
# (C) Jeffery Kline 2011
# GNU/GPL v3
PKGNAME=python-pmdc
VERSION=1.0.1

# RPMDIR is used by Scientific Linux
#        causes no harm on Debian.
RPMDIR=$(HOME)/rpmbuild

help: 
	@echo
	@echo
	@echo "Targets are:"
	@echo "    help, debconf_update, chk_ver, clean, install, srpm, rpm and deb"
	@echo
	@echo "help: print this message"
	@echo "chk_ver: check that versions agree"
	@echo "debconf_update: run commands that debconf needs to mitigate complaints"
	@echo "clean: remove stuff"
	@echo "install: run the command" 
	@echo "    python setup.py install --root=DESTDIR --prefix=/usr"
	@echo "srpm: clean and then build the source rpm"
	@echo "rpm: clean and then build the rpm and source rpm"
	@echo "deb: debuild -uc -us"
	@echo
	@echo

clean:
	find . -name '*~' -delete
	find . -name '*.pyc' -delete
	$(RM) -r\
		$(RPMDIR)/*/$(PKGNAME)*\
		$(RPMDIR)/*/*/$(PKGNAME)*\
		../$(PKGNAME)_*\
		build\
		dist\
		MANIFEST\
		debian/$(PKGNAME)\
		debian/$(PKGNAME).*.log\
		debian/$(PKGNAME).*.debhelper\
		debian/$(PKGNAME).substvars\
		debian/tmp\
		debian/files\
		debian/debhelper.*

# debian/rules does not use this; see debian/rules
install: chk_ver
	python setup.py install --root=${DESTDIR} --prefix=/usr

# Scientific Linux source rpm
srpm: chk_ver clean
	@# copy the local specfile to the necessary place
	install $(PKGNAME).spec $(RPMDIR)/SPECS/
	@# create the tar.gz file 
	tar -zcf\
		$(RPMDIR)/SOURCES/$(PKGNAME)-$(VERSION).tar.gz\
		--transform=s/\./$(PKGNAME)-$(VERSION)/\
		--exclude-vcs\
		.
	@# build the source from the specfile
	rpmbuild -bs $(RPMDIR)/SPECS/$(PKGNAME).spec

rpm: srpm
	@# build all
	rpmbuild -ba $(RPMDIR)/SPECS/$(PKGNAME).spec

chk_ver:
	@echo "Version of ${PKGNAME} should be $(VERSION)"
	@echo -n "  Checking Debian, use file 'debian/changelog'."
	@head -n1 debian/changelog | grep '($(VERSION))' > /dev/null
	@echo "..Debian OK."
	@echo -n "  Checking Scientific Linux Version, use file '${PKGNAME}.spec'."
	@if [ -f $(PKGNAME).spec ]; then\
	  grep -E '%define version +$(VERSION)' $(PKGNAME).spec > /dev/null;\
	else\
	  echo "..No file $(PKGNAME).spec. Silently continuing.";\
	fi
	@echo "..SL OK."
	@echo -n "  Checking Python Version, use file ./setup.py'.";
	@grep -E "version[[:space:]]*=[[:space:]]*[[:punct:]]$(VERSION)[[:punct:]]" ./setup.py > /dev/null
	@echo "..Python OK."


# debian commands to build deb files
deb: chk_ver
	debuild -rfakeroot -D -uc -us -I
