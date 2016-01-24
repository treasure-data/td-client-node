all:
	npm install

.PHONY: test clean
test:
	./node_modules/.bin/mocha

circleci:
	multi='dot=- html-cov=${CIRCLE_ARTIFACTS}/coverage.html' ./node_modules/mocha/bin/mocha -r blanket --reporter mocha-multi
	./node_modules/.bin/jsdoc -d ${CIRCLE_ARTIFACTS}/apidoc -R ./README.md ./lib/index.js

clean:
	rm -rf ./docs node_modules ./test/coverage.html

site: ./lib/index.js
	./node_modules/.bin/jsdoc -d ./docs -R ./README.md ./lib/index.js

