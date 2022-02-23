setup/dependencies:
	python3 setup.py develop -x

test:
	tox --develop
test/watch:
	ptw -c

version/bump/major:
	bump2version major
version/bump/minor:
	bump2version minor
version/bump/patch:
	bump2version patch
version/bump/release:
	bump2version release
version/bump/build:
	bump2version build