.PHONY: test
test:
	NEO4J_BOLT=bolt://localhost:7687 go test -v -coverprofile=cover.out

.PHONY: coverage
coverage: test
	go tool cover -html=cover.out

.PHONY: clean
clean:
	rm -f cover.out

.PHONY: godoc
godoc:
	godoc -http=":6060"

.PHONY: neo4j
neo4j:
	docker run --publish=8474:7474 --publish=8687:7687 --env=NEO4J_AUTH=none --rm neo4j:3.1.0
