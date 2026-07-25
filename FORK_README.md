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
- **`\trc`** - Show tables with row counts (estimated + max id)

### Schema and Table Introspection

- **`\it [pattern]`** - Search tables by name pattern in information_schema
  - Alphanumeric patterns auto-wrap with `%`: `\it user` → `%user%`
  - Use `*` for explicit wildcards: `\it user*` → `user%`
- **`\ic [pattern]`** - Search columns by name pattern in information_schema
  - Same pattern behavior as `\it`
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
- **`\tc [table]`** - Truncate table

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
- **`Ctrl-O`** - Open the current connection in `vdsql` in a floating kitty overlay
  - Connection URL is built from the live connection, so it follows the schema selected via `Ctrl-B`
  - Removes the upstream `Ctrl-O` chords (`C-o d`, `C-o C-d`, `C-o t`, `C-o C-t` — server date/datetime insertion) so the binding fires instantly instead of waiting out prompt_toolkit's 1s chord timeout

### Safety Features

- **UPDATE query validation** - All UPDATE queries must have a WHERE clause
  - Throws `UpdateWithoutWhereError` exception if WHERE clause is missing
  - Prevents accidental mass updates affecting all table rows

### Pager Integration

- **Drill from visidata-db** - While visidata-db is open, drill operations run live against the database and load straight back into the open viewer — no need to close the pager.
  - Select a row → press `{` (drill up) or `}` (drill down) to navigate the hierarchy of the active table
  - Open a related table by id without leaving visidata-db (open-table action)
- **How it works** - When visidata-db is launched as the pager, the CLI starts a per-process Unix-domain **socket server** on a daemon thread and exports its path via the `DB_SOCKET` environment variable. visidata-db connects to that socket and sends a newline-terminated JSON request (`{"action": "drill_up"|"drill_down"|"open_table", ...}`); the CLI runs the corresponding drill query on the live connection and returns CSV. Because the CLI's main thread is blocked inside the pager while it is open, the socket handler can safely reuse the live database connection.
  - The socket server is started lazily on first pager use and shut down when the CLI exits.
  - This replaces the deprecated reply-file mechanism (`/tmp/rlocal/visidata/last-reply`), which required closing the pager so the CLI could re-execute `\du`/`\dd` on the next prompt iteration.
- **Drill context** - The active table for drill operations is tracked from the last drill command (`\do`, `\du`, etc.) and from plain `select ... from <table>` queries, so you can run an ordinary query and immediately drill into its rows from visidata-db. Qualified `schema.table` references remember the schema, so a later unqualified open-table reuses it.

### Autocompletion

Custom commands support intelligent autocompletion:

- Table name completion for all drill commands
- Column name completion for `\dc` command after table name
- Uses existing CLI completion infrastructure

### Environment Variables

- **`USE_MINIMAL_COLUMN_SET`** - When set to "1", filters columns to minimal set: `id`, `parent_id`, `level`, `kode`, `code`, `nama`, `name`
- **`PAGER`** - Can be set to "visidata-db" for custom tabular data viewing
- **`DB_SOCKET`** - Set by the CLI (not the user) to the path of the per-process drill socket while visidata-db is open; visidata-db reads it to send live drill/open-table requests back to the CLI
- **`DBCONFIG_ID`** - Used for schema persistence cache file naming

### External Tool Integration

This fork integrates with several external tools for enhanced workflows:

- **rofi** - Schema selection menu
- **i3-msg** - Window management integration for `\sct` command
- **visidata-db** - Custom pager for tabular data with interactive drill operations

---

## MySQL-Specific Features

### Patched Commands

- **`\watch`** - Clears screen between iterations, forces ASCII format, disables pager, shows gray timestamp status

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
