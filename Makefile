.PHONY: test

test:
	./mvnw clean verify

build:
	./mvnw clean install

publish-central:
	./mvnw -e clean deploy -P ossrh