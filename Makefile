all:
	npm install

.PHONY: test clean
test:
	npm test

circleci:
	npm test
	npm run coverage
	cp -R coverage ${CIRCLE_ARTIFACTS}/coverage
	./node_modules/.bin/jsdoc -d ${CIRCLE_ARTIFACTS}/apidoc -R ./README.md ./lib/index.js

clean:
	rm -rf ./docs node_modules ./test/coverage.html

site: ./lib/index.js
	./node_modules/.bin/jsdoc -d ./docs -R ./README.md ./lib/index.js
