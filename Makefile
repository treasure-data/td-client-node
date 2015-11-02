all:
	npm install

.PHONY: test clean
test:
	./node_modules/.bin/mocha

clean:
	rm -rf ./docs node_modules

site: ./lib/index.js
	./node_modules/.bin/jsdoc -d ./docs -R ./README.md ./lib/index.js

