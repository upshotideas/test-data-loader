.PHONY: test

test:
	./mvnw clean verify

build:
	./mvnw clean install

publish-central:
	./mvnw clean deploy -P ossrh