# ExPgQuery

Elixir library with a C NIF for parsing PostgreSQL queries. Very much inspired by
[pganalyze/pg_query](https://github.com/pganalyze/pg_query), and utilizes
[pganalyze/libpg_query](https://github.com/pganalyze/libpg_query) for turning queries
into a parsetree, normalizing and fingerprinting.

### Features

- Comprehensive SQL query analysis:
  - Extract referenced tables (SELECT/DML/DDL)
  - Identify Common Table Expressions (CTEs)
  - List function calls and their types
  - Find columns used in filter conditions (`WHERE`, `JOIN ... ON`, etc.)
- Query manipulation:
  - Smart query truncation
  - Query fingerprinting for identifying similar queries
  - Query normalization (replacing literals with placeholders)

## Installation

Not published to Hex yet.

## Usage

### Basic Parsing

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
```

### Query Categories

```elixir
# Extract tables by operation type
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

### Function Analysis

```elixir
# Analyze function usage
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

### Query Normalization & Fingerprinting

```elixir
# Normalize query by replacing literals with placeholders
iex> ExPgQuery.Normalize.normalize("SELECT * FROM users WHERE id = 123")
{:ok, "SELECT * FROM users WHERE id = $1"}

# Generate fingerprint to identify similar queries
iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 123")
{:ok, "a0ead580058af585"}

iex> ExPgQuery.Fingerprint.fingerprint("SELECT * FROM users WHERE id = 456")
{:ok, "a0ead580058af585"}
```

### Query Truncation

```elixir
# Intelligently truncate long queries
iex> query = "SELECT very, many, columns FROM a_table WHERE x > 1"
iex> {:ok, tree} = ExPgQuery.Protobuf.from_sql(query)
iex> ExPgQuery.Truncator.truncate(tree, 34)
{:ok, "SELECT ... FROM a_table WHERE ..."}
```

## Documentation

Detailed documentation with more examples and API references is available in the module docs.

## Contributing

Bug reports and pull requests are welcome.
