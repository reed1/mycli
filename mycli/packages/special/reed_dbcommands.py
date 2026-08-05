import csv
import io
import json
import logging
import os
import re
import subprocess

from .main import special_command, ArgType
from mycli.packages.sqlresult import SQLResult

log = logging.getLogger(__name__)

DB_SOCKET_ENV = "DB_SOCKET"

# Track the last tabular command context
_last_tabular_command_table = None
# Last schema seen on a qualified table; preserved across unqualified references
# so "open table" can reuse it (select * from <schema>.<table> ...).
_last_schema = None

# One socket server per mycli process, started lazily when first needed.
_socket_server = None


def reed_tabular_command(func):
    """Track the table a tabular command was last invoked on, for drill context."""

    def wrapper(*args, **kwargs):
        global _last_tabular_command_table, _last_schema

        if kwargs.get("arg"):
            arg_parts = re.split(r"\s+", kwargs["arg"].strip())
            if arg_parts and arg_parts[0]:
                _last_tabular_command_table = arg_parts[0]
                if "." in arg_parts[0]:
                    _last_schema = arg_parts[0].split(".", 1)[0]

        return func(*args, **kwargs)

    return wrapper


def set_active_table_from_sql(sql):
    """Record the first table referenced by a plain SQL query as the drill context.

    Lets `select * from <table>` feed drill up/down just like the \\do family.
    Reuses mycli's own table extractor; backslash commands yield no tables and
    leave the context untouched.
    """
    global _last_tabular_command_table, _last_schema

    from mycli.packages.parseutils import extract_tables_from_complete_statements

    tables = extract_tables_from_complete_statements(sql)
    if not tables:
        return
    schema, table, _alias = tables[0]
    if schema:
        _last_schema = schema
    _last_tabular_command_table = f"{schema}.{table}" if schema else table


def _results_to_csv(results):
    out = io.StringIO()
    writer = csv.writer(out)
    for result in results:
        if result.header:
            writer.writerow(result.header)
        if result.rows is not None:
            for row in result.rows:
                writer.writerow(["" if value is None else value for value in row])
    return out.getvalue()


def ensure_socket_server(mycli):
    """Start the per-process socket server on first use and export its path.

    VisiData runs as mycli's pager, so while it is open mycli's main thread is
    blocked in echo_via_pager and never touches the connection — the server can
    safely reuse the live connection to answer drill requests.
    """
    global _socket_server
    if _socket_server is not None:
        return _socket_server

    from mycli.packages.socket_server import SocketServer

    server = SocketServer(lambda request: _handle_request(mycli, request))
    server.start()
    os.environ[DB_SOCKET_ENV] = server.path
    _socket_server = server
    return server


def shutdown_socket_server():
    global _socket_server
    if _socket_server is not None:
        _socket_server.stop()
        _socket_server = None
    os.environ.pop(DB_SOCKET_ENV, None)


def _handle_request(mycli, request):
    req = json.loads(request)
    try:
        csv_text = _run_action(mycli, req)
        return json.dumps({"ok": True}).encode() + b"\n" + csv_text.encode()
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}).encode() + b"\n"


def _run_action(mycli, req):
    action = req["action"]
    cur = mycli.sqlexecute.conn.cursor()

    if action in ("drill_up", "drill_down"):
        table = _last_tabular_command_table
        if not table:
            raise RuntimeError("No active table context to drill from")
        arg = f"{table} {req['id']}"
        results = drill_up(cur, arg=arg) if action == "drill_up" else drill_down(cur, arg=arg)
    elif action == "open_table":
        table = req["table"]
        if _last_schema and "." not in table:
            table = f"{_last_schema}.{table}"
        results = drill_one(cur, arg=f"{table} {req['id']}")
    else:
        raise RuntimeError(f"Unknown action: {action}")

    return _results_to_csv(results)


@special_command(
    "\\d",
    "\\d [table]",
    "Describe table",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def describe(cur, arg=None, **_):
    [table, *args] = re.split(r"\s+", arg)
    query = f"show create table {table}"
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\do",
    "\\do [table] [id]",
    "Get one row",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
@reed_tabular_command
def drill_one(cur, arg=None, **_):
    [table, *args] = re.split(r"\s+", arg)
    if len(args) == 0:
        query = f"select * from {table} limit 100"
    elif args[0].isdigit():
        row_id = int(args[0])
        query = f"select * from {table} where id = {row_id}"
    else:
        extra_clause = " ".join(args)
        if "limit" in extra_clause.lower():
            query = f"select * from {table} {extra_clause}"
        else:
            query = f"select * from {table} {extra_clause} limit 100"
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\du",
    "\\du [table] [id]",
    "Drill up row",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
@reed_tabular_command
def drill_up(cur, arg=None, **_):
    [table, row_id, *args] = re.split(r"\s+", arg)
    cols = get_filtered_columns(cur, table)
    q_cols = ", ".join(cols)
    qc_cols = ", ".join([f"c.{x}" for x in cols])
    query = f"""
    with recursive cte as (
        select {q_cols}, 1 as depth from {table} where id = {row_id}
        union all
        select {qc_cols}, cte.depth + 1 from {table} as c
        inner join cte on c.id = cte.parent_id
    )
    select {q_cols} from cte {' '.join(args)} order by depth desc
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\dd",
    "\\dd [table] [id]",
    "Drill down row",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
@reed_tabular_command
def drill_down(cur, arg=None, **_):
    [table, row_id, *args] = re.split(r"\s+", arg)
    cols = get_filtered_columns(cur, table)
    extra = " ".join(args)
    q_where = "(1=1)"
    if extra.startswith("where "):
        q_where = extra[6:]
    q_cols = ", ".join(cols)
    query = f"""
    select {q_cols}
    from {table}
    where parent_id = {row_id} and
        ({q_where})
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\ddr",
    "\\ddr [table] [id]",
    "Drill down row recursively",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
@reed_tabular_command
def drill_down_recursive(cur, arg=None, **_):
    [table, row_id, *args] = re.split(r"\s+", arg)
    cols = get_filtered_columns(cur, table)
    extra = " ".join(args)
    q_where = "(1=1)"
    if extra.startswith("where "):
        q_where = extra[6:]
    q_cols = ", ".join(cols)
    query = f"""
    with recursive cte as (
        select {q_cols}, 0 as depth from {table} where id = {row_id}
        union all
        select {', '.join([f"c.{col}" for col in cols])}, cte.depth + 1
        from {table} as c
        inner join cte on c.parent_id = cte.id
        where {q_where}
    )
    select depth, {q_cols} from cte order by depth, id
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\dk",
    "\\dk [table] [kode]",
    "Drill down kode",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
@reed_tabular_command
def drill_down_kode(cur, arg=None, **_):
    [table, kode, *args] = re.split(r"\s+", arg)
    t_source = f"select * from {table}"
    if len(args) > 0 and args[0] == "where":
        t_source += " " + " ".join(args)
    cols = get_filtered_columns(cur, table)
    kodes = kode.split(".")
    query = f"""
    with recursive t_source as (
        {t_source}
    ), td as (
        {' union all '.join([
            f"select {i} as depth, '{k}' as kode"
            for i, k in enumerate(kodes)
        ])}
    ),
    t as (
        select {', '.join(cols)}, 0 as depth, cast(kode as char(255)) as kode_full
        from t_source
        where
            parent_id = 0 and
            kode = (select kode from td where depth = 0)
        union all
        select c.{', c.'.join(cols)}, t.depth + 1 as depth, concat(t.kode_full, '.', c.kode) as kode_full
        from t
        inner join t_source as c on
            c.parent_id = t.id and
            c.kode = (select kode from td where depth = t.depth + 1)
    )
    select kode_full, {', '.join(cols)} from t
    order by depth, id
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\tree",
    "\\tree [table] [root_id]",
    "Show tree for a table",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def tree(cur, arg=None, **_):
    [table, *args] = re.split(r"\s+", arg)
    if len(args) == 0:
        where = "(parent_id = 0)"
    else:
        root_id = int(args[0])
        where = f"(id = {root_id})"
    query = f"""
        with recursive cte as (
            select
            id,
            level,
            cast(NULL as char(50)) as parent_level,
            0 as depth
            from {table}
            where {where}
            union all
            select
            t.id,
            t.level,
            cte.level as parent_level,
            cte.depth + 1
            from {table} t
            inner join cte on t.parent_id = cte.id
        )
        select
            depth,
            parent_level,
            level,
            count(*) as cnt
        from cte
        group by depth, parent_level, level
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        rows = list(cur.fetchall())
        formatted_rows = _build_and_format_tree(rows)
        headers = ["depth", "level", "cnt"]
        return [SQLResult(rows=formatted_rows, header=headers)]
    else:
        return [SQLResult()]


def _build_and_format_tree(rows):
    """Build tree structure from rows and format with box-drawing characters."""
    if not rows:
        return rows

    # Build the tree structure: {parent_level: [(level, count), ...]}
    children_map = {}
    depth_map = {}  # {(parent_level, level): depth}

    for depth, parent_level, level, cnt in rows:
        key = parent_level if parent_level else None
        if key not in children_map:
            children_map[key] = []
        children_map[key].append((level, cnt))
        depth_map[(parent_level, level)] = depth

    # Traverse and format the tree
    result = []

    def traverse(parent_level, prefix_parts):
        """Recursively traverse and format tree nodes."""
        children = children_map.get(parent_level, [])

        for i, (level, cnt) in enumerate(children):
            is_last = i == len(children) - 1
            depth = depth_map[(parent_level, level)]

            # Build the prefix for this node
            if depth == 0:
                prefix = ""
            else:
                # Add branch for current node
                if is_last:
                    prefix = "".join(prefix_parts) + "└─ "
                else:
                    prefix = "".join(prefix_parts) + "├─ "

            formatted_level = prefix + level
            result.append((depth, formatted_level, cnt))

            # Recursively traverse children
            if level in children_map:
                # Prepare prefix for children
                if depth == 0:
                    new_prefix_parts = []
                else:
                    if is_last:
                        new_prefix_parts = prefix_parts + ["   "]
                    else:
                        new_prefix_parts = prefix_parts + ["│  "]
                traverse(level, new_prefix_parts)

    # Start traversal from root (parent_level = None)
    traverse(None, [])

    return result


@special_command(
    "\\it",
    "\\it <pattern>",
    "Search tables by name pattern",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def info_tables(cur, arg=None, **_):
    pattern = arg.strip() if arg else "%"
    if pattern.isalnum():
        pattern = f"%{pattern}%"
    else:
        pattern = pattern.replace("*", "%")
    query = f"select * from information_schema.tables where table_name like '{pattern}'"
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\ic",
    "\\ic <pattern>",
    "Search columns by name pattern",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def info_columns(cur, arg=None, **_):
    pattern = arg.strip() if arg else "%"
    if pattern.isalnum():
        pattern = f"%{pattern}%"
    else:
        pattern = pattern.replace("*", "%")
    query = (
        f"select * from information_schema.columns where column_name like '{pattern}'"
    )
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\gcol",
    "\\gcol <table>",
    "Get columns",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def get_columns(cur, arg=None, **_):
    table = re.split(r"\s+", arg)[0]
    q_where_schema = "(table_schema = database())"
    if "." in table:
        schema = table.split(".")[0]
        table = table.split(".")[-1]
        q_where_schema = f"(table_schema = '{schema}')"
    query = f"""
    select
        column_name as name,
        data_type as type
    from information_schema.columns
    where table_name = '{table}' and {q_where_schema}
    order by ordinal_position
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\dc",
    "\\dc [table] [columns]",
    "Get distinct count of columns",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def get_distinct_count(cur, arg=None, **_):
    if not re.match(r"^\w+(\s+\"?\w+\"?)+$", arg):
        raise ValueError(r"Invalid pattern. Should be \\dc table [columns]..")
    [table, *columns] = re.split(r"\s+", arg)
    cols = ", ".join(columns)
    query = (
        f"select {cols}, count(*) as cnt from {table} group by {cols} order by {cols}"
    )
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [SQLResult(rows=cur, header=headers)]
    else:
        return [SQLResult()]


@special_command(
    "\\lt",
    "\\lt '<path>' <table>",
    "Load data from file into table",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def load_table(cur, arg=None, **_):
    # Match file path in quotes and table name
    match = re.match(r"^'([^']+)'\s+(\w+)$", arg.strip())
    if not match:
        raise ValueError(r"Invalid pattern. Should be \\lt '<path>' <table>")

    file_path = match.group(1)
    table = match.group(2)

    query = f"""load data local infile '{file_path}'
into table {table}
fields terminated by ',' enclosed by '"'
escaped by '' lines terminated by '\\n'
ignore 1 lines"""

    log.debug(query)
    cur.execute(query)

    # Get the number of rows affected
    rows_affected = cur.rowcount
    status_message = f"Query OK, {rows_affected} rows affected"

    return [SQLResult(status=status_message)]


@special_command(
    "\\tc",
    "\\tc [table...]",
    "Truncate table",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def truncate_table(cur, arg=None, **_):
    tables = re.split(r"\s+", arg)
    for table in tables:
        query = f"truncate table {table}"
        log.debug(query)
        cur.execute(query)
    status_message = f"Truncated {len(tables)} table(s) successfully"
    return [SQLResult(status=status_message)]


@special_command(
    "\\ss",
    "\\ss[+] [schema]",
    "Select schema",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def select_schema(cur, arg=None, **_):
    schema = arg
    if not schema:
        query = "SELECT schema_name FROM information_schema.schemata"
        log.debug(query)
        cur.execute(query)
    if schema:
        query = f"use {schema}"
        log.debug(query)
        cur.execute(query)
        return [SQLResult()]
    else:
        return [SQLResult()]


@special_command(
    "\\sct",
    "\\sct [table]",
    "Show create table",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def show_create_table(cur, arg=None, **_):
    [table, *args] = re.split(r"\s+", arg)
    query = f"show create table {table}"
    log.debug(query)
    cur.execute(query)
    rows = cur.fetchall()
    headers = [x[0] for x in cur.description]
    if "Create Table" in headers:
        ct_idx = headers.index("Create Table")
    elif "Create View" in headers:
        ct_idx = headers.index("Create View")
    else:
        raise ValueError("No create table or view found")
    content = rows[0][ct_idx]
    with open("/tmp/sct_query.sql", "w") as f:
        f.write(content)
    subprocess.run(
        ["kitty", "@", "launch", "--type=overlay", "show-sql", "-l", "mysql", "/tmp/sct_query.sql"],
        check=True,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    return [SQLResult()]


def get_filtered_columns(cur, table):
    query = f"show fields from {table}"
    log.debug(query)
    cur.execute(query)
    columns = [x[0] for x in cur.fetchall()]
    if os.environ.get("USE_MINIMAL_COLUMN_SET", "0") == "1":
        minimal_column_set = set(
            ["id", "parent_id", "level", "kode", "code", "nama", "name"]
        )
        return [x for x in columns if x in minimal_column_set]
    else:
        return columns


@special_command(
    "\\df",
    "\\df [recipe]",
    "Directed format - set pager and table format",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def directed_format(cur, arg=None, **kwargs):
    from .iocommands import set_pager_enabled
    from .main import execute

    recipe = arg.strip().upper() if arg else "A"

    if recipe == "A":
        # Recipe A: visidata-db pager with CSV format
        os.environ["PAGER"] = "visidata-db"
        set_pager_enabled(True)
        list(execute(cur, "\\T csv"))
        return [SQLResult(status="Directed format A: pager=visidata-db, format=csv")]
    elif recipe == "C":
        # Recipe C: no pager with ASCII format
        set_pager_enabled(False)
        list(execute(cur, "\\T ascii"))
        return [SQLResult(status="Directed format C: pager=disabled, format=ascii")]
    else:
        return [SQLResult(status=f"Unknown recipe '{recipe}'. Use A or C.")]


@special_command(
    "\\trc",
    "\\trc",
    "Show tables with row counts (estimated + max id)",
    arg_type=ArgType.PARSED_QUERY,
    case_sensitive=True,
)
def table_row_count(cur, arg=None, **_):
    # Step 1: Get tables and check for numeric id column
    cur.execute("""
    SELECT t.table_name,
           MAX(CASE WHEN c.column_name = 'id'
               AND c.data_type IN ('int', 'bigint', 'smallint', 'mediumint', 'tinyint') THEN 1 ELSE 0 END) as has_id
    FROM information_schema.tables t
    LEFT JOIN information_schema.columns c
        ON t.table_name = c.table_name
        AND t.table_schema = c.table_schema
        AND c.column_name = 'id'
    WHERE t.table_schema = database()
        AND t.table_type = 'BASE TABLE'
    GROUP BY t.table_name
    ORDER BY t.table_name
    """)
    tables = list(cur.fetchall())

    if not tables:
        return [SQLResult()]

    # Step 2: Build UNION ALL for max(id)
    tables_with_id = [t[0] for t in tables if t[1] == 1]

    max_id_map = {}
    if tables_with_id:
        union_parts = [
            f"SELECT '{t}' as table_name, MAX(id) as max_id FROM `{t}`"
            for t in tables_with_id
        ]
        max_id_query = " UNION ALL ".join(union_parts)
        cur.execute(max_id_query)
        for row in cur.fetchall():
            max_id_map[row[0]] = row[1]

    # Step 3: Get estimated row counts from information_schema
    cur.execute("""
    SELECT table_name, table_rows
    FROM information_schema.tables
    WHERE table_schema = database()
        AND table_type = 'BASE TABLE'
    """)
    stat_map = {}
    for row in cur.fetchall():
        stat_map[row[0]] = row[1]

    # Step 4: Combine results
    combined_rows = []
    for table_name, has_id in tables:
        est_count = stat_map.get(table_name, 0)
        max_id = max_id_map.get(table_name)
        combined_rows.append((table_name, est_count, max_id))

    headers = ["table_name", "est_count", "max_id"]
    return [SQLResult(header=headers, rows=combined_rows, status=f"SELECT {len(combined_rows)}")]


def is_reed_command(cmd):
    """Check if a command is one of Reed's special commands."""
    return cmd in (
        "\\d",
        "\\do",
        "\\dd",
        "\\du",
        "\\ddr",
        "\\dk",
        "\\tree",
        "\\it",
        "\\ic",
        "\\gcol",
        "\\dc",
        "\\lt",
        "\\tc",
        "\\ss",
        "\\sct",
        "\\df",
        "\\trc",
    )


def reed_suggestions(cmd, arg):
    """Provide context-aware completions for Reed's special commands."""
    if not arg or not arg.strip():
        # No argument yet, suggest tables for most commands
        if cmd == "\\ss":
            # For schema selection, suggest schemas instead of tables
            return [{"type": "schema"}]
        elif cmd == "\\df":
            # For directed format, suggest recipe options
            return [{"text": "A"}, {"text": "C"}]
        else:
            # For other commands, suggest tables
            return [{"type": "table", "schema": []}, {"type": "schema"}]
    else:
        # Check if we're still on the first argument
        args = arg.split()
        if len(args) == 1 and not arg.endswith(" "):
            # Still typing the first argument
            if cmd == "\\ss":
                # Schema selection
                return [{"type": "schema"}]
            else:
                # Table name completion
                if "." in args[0]:
                    # Schema-qualified table
                    schema = args[0].split(".")[0]
                    return [{"type": "table", "schema": schema}]
                else:
                    return [{"type": "table", "schema": []}, {"type": "schema"}]

        # For commands that need column names after the table
        elif cmd == "\\dc":
            if len(args) >= 1 and (arg.endswith(" ") or len(args) > 1):
                # Already have table name, suggest columns for grouping
                table_name = args[0]
                if "." in table_name:
                    schema, table = table_name.split(".", 1)
                    table_tuple = (schema, table, None)
                else:
                    table_tuple = (None, table_name, None)
                return [{"type": "column", "tables": [table_tuple]}]

        # For commands that take table + additional arguments (but not columns)
        elif cmd in ("\\do", "\\du", "\\dd", "\\ddr", "\\dk", "\\tree", "\\tc"):
            # These commands take table name + other args, but we don't complete the other args
            # So return empty suggestions for additional arguments
            return []

        # For \lt command - no suggestions after the path argument
        elif cmd == "\\lt":
            # This command takes a quoted path and table name, no completion needed
            return []

        # For \df command - suggest recipe options
        elif cmd == "\\df":
            if len(args) == 0 or (len(args) == 1 and not arg.endswith(" ")):
                # Suggest recipe options
                return [{"text": "A"}, {"text": "C"}]
            return []

    return []
