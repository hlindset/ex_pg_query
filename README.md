# ExPgQuery

Elixir library with a C NIF for parsing PostgreSQL queries. Very much inspired by
[pganalyze/pg_query](https://github.com/pganalyze/pg_query), and utilizes
[pganalyze/libpg_query](https://github.com/pganalyze/libpg_query) for turning queries
into a parsetree, normalizing and fingerprinting.

### Features

- Extract SQL query information:
  - Referenced tables
  - Common table expressions
  - Function calls
  - Columns used in filter conditions (`WHERE`, `JOIN ... ON`, etc.)
- Query manipulation:
  - Smart query truncation
  - Query fingerprinting for identifying structurally equivalent queries
  - Query normalization (replacing literals with placeholders)

## Installation

Not published to Hex yet.

## Usage

### Basic Parsing Example

```elixir
# Parse a query and analyze its structure
iex> query = "SELECT u.name FROM users u WHERE u.age > 21"
iex> {:ok, result} = ExPgQuery.parse(query)

# Get referenced tables
iex> ExPgQuery.tables(result)
["users"]

# Get table aliases
iex> ExPgQuery.table_aliases(result)
[%{alias: "u", relation: "users", location: 19, schema: nil}]

# Get filter columns
iex> ExPgQuery.filter_columns(result)
[{"users", "age"}]

# Get statement types
iex> ExPgQuery.statement_types(result)
[:select_stmt]
```

### Extract Table References

Extract table references by operation type.

```elixir
iex> {:ok, result} = ExPgQuery.parse("""
...>   SELECT * FROM users;
...>   CREATE TABLE posts (id int);
...>   INSERT INTO comments (text) VALUES ('hello');
...> """)

iex> ExPgQuery.select_tables(result)
["users"]

iex> ExPgQuery.ddl_tables(result)
["posts"]

iex> ExPgQuery.dml_tables(result)
["comments"]

# Even within the same query
iex> {:ok, result} = ExPgQuery.parse("""
...>   SELECT * INTO films_recent
...>   FROM films
...> """)

iex> ExPgQuery.select_tables(result)
["films"]

iex> ExPgQuery.ddl_tables(result)
["films_recent"]
```

### Extract Function References

Extract function references by operation type.

```elixir
iex> {:ok, result} = ExPgQuery.parse("""
...>   SELECT count(*), my_func(col) FROM users;
...>   CREATE FUNCTION add(a int, b int) RETURNS int;
...> """)

iex> ExPgQuery.functions(result)
["add", "my_func", "count"]

iex> ExPgQuery.call_functions(result)
["my_func", "count"]

iex> ExPgQuery.ddl_functions(result)
["add"]
```

### Query Normalization

Normalize queries by replacing literals with placeholders.

```elixir
iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE id = 123")
{:ok, "SELECT * FROM users WHERE id = $1"}
```

### Fingerprinting

Generate fingerprints for queries to identify structurally equivalent queries.

```elixir
iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 123")
{:ok, "a0ead580058af585"}

iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 456")
{:ok, "a0ead580058af585"}
```

### Query Truncation

Intelligently truncate long queries.

```elixir
iex> query = "SELECT very, many, columns FROM a_table WHERE x > 1"
iex> {:ok, tree} = ExPgQuery.Protobuf.from_sql(query)
iex> ExPgQuery.Truncator.truncate(tree, 34)
{:ok, "SELECT ... FROM a_table WHERE ..."}
```

## Contributing

Bug reports and pull requests are welcome.
