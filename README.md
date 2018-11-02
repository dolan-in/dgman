***Dgman*** is a schema manager for [Dgraph](https://dgraph.io/), which manages Dgraph schema and indexes from Golang tags in struct definitions.

## Features
- Create schemas and indexes from struct tags.
- Detect conflicts from existing schema and defined schema.
- Autoinject [node type](https://docs.dgraph.io/howto/#giving-nodes-a-type) from struct.

## Roadmap
- Field unique checking (e.g: emails, username).
- Query from structs.
- Query helpers for filtering.
