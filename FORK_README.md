# mycli Fork

This is a customized fork of [mycli](https://github.com/dbcli/mycli) with enhanced features for database exploration, hierarchical data navigation, and improved workflow integration.

## Hierarchical Data Navigation

Commands for working with tables that have parent-child relationships (using `id`/`parent_id` columns):

- **`\du [table] [id]`** - Drill up: Recursively traverse up the hierarchy from a given row
- **`\dd [table] [id]`** - Drill down: Query immediate children of a given row
- **`\ddr [table] [id]`** - Drill down recursive: Recursively traverse entire subtree from a given row
- **`\dk [table] [kode]`** - Drill down by kode: Navigate hierarchical data using dot-separated kode paths (e.g., "01.02.03")
- **`\do [table] [id]`** - Drill one: Get one or more rows by ID, supports custom WHERE clauses and LIMIT
- **`\tree [table] [root_id]`** - Display hierarchical tree structure with visual indentation

## Schema and Table Introspection

- **`\d [table]`** - Describe table (show create table)
- **`\gcol [table]`** - Get columns list for a table with data types from information_schema
- **`\dc [table] [columns]`** - Get distinct count grouped by specified columns
- **`\sct [table]`** - Show create table in external terminal window (i3wm integration)
- **`\ss [schema]`** - Select/switch database schema (lists all schemas if no argument provided)

## Data Manipulation

- **`\lt '<path>' [table]`** - Load CSV data from file into table using LOAD DATA LOCAL INFILE

## Output Formatting

- **`\df [recipe]`** - Directed format: Set pager and output format recipes
  - Recipe A: visidata-db pager with CSV format (default)
  - Recipe C: no pager with ASCII format

## Custom Key Bindings

- **`Ctrl-E`** - Edit current input in external editor
- **`Ctrl-B`** - Interactive schema selector using rofi
  - Lists all schemas with custom sorting (alphabetical for letters, reverse numerical for digits)
  - Persists last selected schema to `~/.cache/rlocal/db/{DBCONFIG_ID}.last_schema`
  - Auto-generates and executes USE statement

## Connection Management

- **Connection keepalive thread** - Background daemon that pings MySQL connection every 30 seconds to prevent timeout during long idle periods

## Safety Features

- **UPDATE query validation** - All UPDATE queries must have a WHERE clause
  - Throws `UpdateWithoutWhereError` exception if WHERE clause is missing
  - Prevents accidental mass updates affecting all table rows

## Pager Integration

- **Command scheduling from pager** - Integration with visidata-db for interactive drill operations
  - Tracks last tabular command context using `@reed_tabular_command` decorator
  - Reads reply file at `/tmp/rlocal/visidata/last-reply` after pager closes
  - Supports `drill_up.<id>` and `drill_down.<id>` reply formats
  - Auto-executes corresponding `\du` or `\dd` commands based on pager interaction

## Environment Variables

- **`USE_MINIMAL_COLUMN_SET`** - When set to "1", filters columns to minimal set: `id`, `parent_id`, `level`, `kode`, `code`, `nama`, `name`
- **`PAGER`** - Can be set to "visidata-db" for custom tabular data viewing
- **`DBCONFIG_ID`** - Used for schema persistence cache file naming

## External Tool Integration

This fork integrates with several external tools for enhanced workflows:

- **rofi** - Schema selection menu
- **i3-msg** - Window management integration
- **visidata-db** - Custom pager for tabular data with interactive drill operations
