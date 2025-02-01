# ExPgQuery

Elixir library with a C NIF for parsing PostgreSQL queries. Very much inspired by [pganalyze/pg_query](https://github.com/pganalyze/pg_query), and utilizes [pganalyze/libpg_query](https://github.com/pganalyze/libpg_query) for turning queries into a parsetree.

### Currently supports
* Converting queries to/from a protobuf based parsetree
* Parsing queries: outputting tables, table aliases, CTEs, functions and filter columns
* Smart query truncation
* Query fingerprinting

## Installation

Not published to Hex yet.
