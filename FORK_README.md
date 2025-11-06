# mycli Fork

This is a customized fork of [mycli](https://github.com/dbcli/mycli) with enhanced features for database exploration, hierarchical data navigation, and improved workflow integration.

---

## Shared Features

> **⚠️ NOTE**: This section is identical in both the [mycli fork](https://github.com/dbcli/mycli) and [pgcli fork](https://github.com/dbcli/pgcli). When updating features here, please keep both README files synchronized.

### Hierarchical Data Navigation

Commands for working with tables that have parent-child relationships (using `id`/`parent_id` columns):

- **`\du [table] [id]`** - Drill up: Recursively traverse up the hierarchy from a given row
- **`\dd [table] [id]`** - Drill down: Query immediate children of a given row
- **`\ddr [table] [id] [where ...]`** - Drill down recursive: Recursively traverse down the hierarchy from a given row
  - `\ddr categories 1` - Get all descendants of row with id = 1
  - `\ddr categories 1 where level < 3` - Get descendants with additional WHERE conditions
- **`\dk [table] [kode]`** - Drill down by kode: Navigate hierarchical data using dot-separated kode paths (e.g., "01.02.03")
- **`\do [table] [id|order by...]`** - Drill one: Get single row by ID, or query with ORDER BY and optional LIMIT
  - `\do users` - Get all rows with LIMIT 100
  - `\do users 123` - Get row with id = 123
  - `\do users order by created_at desc` - Get rows sorted (auto-adds LIMIT 100)
  - `\do users order by created_at desc limit 50` - Custom limit
- **`\tree [table] [root_id]`** - Display hierarchical tree structure with visual indentation

### Schema and Table Introspection

- **`\gcol [table]`** - Get columns list for a table with data types from information_schema
- **`\dc [table] [columns]`** - Get distinct count grouped by specified columns
  - Example: `\dc users country city` shows count of users per country/city combination
- **`\sct [table]`** - Show create table in external terminal window
  - Displays complete DDL in floating i3wm terminal with syntax highlighting

### Data Manipulation

- **`\lt '<path>' [table]`** - Load CSV data from file into table
  - File path must be in single quotes
  - CSV format with header row expected
  - Example: `\lt '/tmp/data.csv' users`

### Output Formatting

- **`\df [recipe]`** - Directed format: Set pager and output format recipes
  - Recipe A: visidata-db pager with CSV format (default)
  - Recipe C: no pager with ASCII format

### Custom Key Bindings

- **`Ctrl-E`** - Edit current input in external editor
- **`Ctrl-B`** - Interactive schema selector using rofi
  - Custom sorting: alphabetical for letters, reverse numerical for digits (recent years first)
  - Persists last selected schema to `~/.cache/rlocal/db/{DBCONFIG_ID}.last_schema`
  - Auto-generates and executes schema switch statement

### Connection Management

- **Connection keepalive thread** - Background daemon that pings database connection every 30 seconds to prevent timeout during long idle periods

### Safety Features

- **UPDATE query validation** - All UPDATE queries must have a WHERE clause
  - Throws `UpdateWithoutWhereError` exception if WHERE clause is missing
  - Prevents accidental mass updates affecting all table rows

### Pager Integration

- **Command scheduling from pager** - Integration with visidata-db for interactive drill operations
  - Tracks last tabular command context using `@reed_tabular_command` decorator
  - Reads reply file at `/tmp/rlocal/visidata/last-reply` after pager closes
  - Supports `drill_up.<id>` and `drill_down.<id>` reply formats
  - Auto-executes corresponding `\du` or `\dd` commands based on pager interaction

### Autocompletion

Custom commands support intelligent autocompletion:

- Table name completion for all drill commands
- Column name completion for `\dc` command after table name
- Uses existing CLI completion infrastructure

### Environment Variables

- **`USE_MINIMAL_COLUMN_SET`** - When set to "1", filters columns to minimal set: `id`, `parent_id`, `level`, `kode`, `code`, `nama`, `name`
- **`PAGER`** - Can be set to "visidata-db" for custom tabular data viewing
- **`DBCONFIG_ID`** - Used for schema persistence cache file naming

### External Tool Integration

This fork integrates with several external tools for enhanced workflows:

- **rofi** - Schema selection menu
- **i3-msg** - Window management integration for `\sct` command
- **visidata-db** - Custom pager for tabular data with interactive drill operations

---

## MySQL-Specific Features

### Database/Schema Commands

- **`\d [table]`** - Describe table (show create table statement)
- **`\ss [schema]`** - Select/switch database schema
  - Lists all schemas if no argument provided
- **`Ctrl-B`** schema selector:
  - Lists all available databases with custom sorting
  - Persists last selected schema to `~/.cache/rlocal/db/{DBCONFIG_ID}.last_schema`
  - Auto-generates and executes `USE` statement

### Implementation Details

- **`\lt`** - Uses MySQL's `LOAD DATA LOCAL INFILE` for CSV loading
- **`\sct`** - Uses `SHOW CREATE TABLE` to extract table DDL

### Additional Tools

- **mysqldump** - Available for table DDL extraction (alternative method)
