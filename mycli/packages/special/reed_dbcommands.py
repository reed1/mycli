import logging
import os
import re
import subprocess

from .main import special_command, PARSED_QUERY

log = logging.getLogger(__name__)


@special_command(
    "\\d",
    "\\d [table]",
    "Describe table",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
def describe(cur, arg=None, **_):
    [table, *args] = re.split(r"\s+", arg)
    query = f"show create table {table}"
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\do",
    "\\do [table] [id]",
    "Get one row",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
def drill_one(cur, arg=None, **_):
    [table, *args] = re.split(r"\s+", arg)
    if len(args) == 0:
        query = f"select * from {table} limit 100"
    elif len(args) == 1:
        row_id = int(args[0])
        query = f"select * from {table} where id = {row_id}"
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\du",
    "\\du [table] [id]",
    "Drill up row",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
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
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\dd",
    "\\dd [table] [id]",
    "Drill down row",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
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
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\ddr",
    "\\ddr [table] [id]",
    "Drill down row recursively",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
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
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\dk",
    "\\dk [table] [kode]",
    "Drill down kode",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
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
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\tree",
    "\\tree [table] [root_id]",
    "Show tree for a table",
    arg_type=PARSED_QUERY,
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
            select id, parent_id,
            cast(level as binary) as level_full,
            level, 0 as depth
            from {table}
            where {where}
            union all
            select t.id, t.parent_id,
            concat(cte.level_full, '-', t.level) as level_full,
            t.level, cte.depth + 1
            from {table} t
            inner join cte on t.parent_id = cte.id
        )
        select
            depth,
            concat(
                repeat('*', depth),
                case when depth > 0 then ' ' else '' end,
                level) as level,
            count(*) as cnt
        from cte
        group by depth, level
        order by min(level_full)
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\gcol",
    "\\gcol <table>",
    "Get columns",
    arg_type=PARSED_QUERY,
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
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\dc",
    "\\dc [table] [columns]",
    "Get distinct count of columns",
    arg_type=PARSED_QUERY,
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
        return [(None, cur, headers, "")]
    else:
        return [(None, None, None, "")]


@special_command(
    "\\lt",
    "\\lt '<path>' <table>",
    "Load data from file into table",
    arg_type=PARSED_QUERY,
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

    return [(None, None, None, status_message)]


@special_command(
    "\\ss",
    "\\ss[+] [schema]",
    "Select schema",
    arg_type=PARSED_QUERY,
    case_sensitive=True,
)
def select_schema(cur, arg=None, arg_type=PARSED_QUERY, verbose=False):
    schema = arg
    if not schema:
        query = "SELECT schema_name FROM information_schema.schemata"
        log.debug(query)
        cur.execute(query)
    if schema:
        query = f"use {schema}"
        log.debug(query)
        cur.execute(query)
        return [(None, None, None, None)]
    else:
        return [(None, None, None, None)]


@special_command(
    "\\sct",
    "\\sct [table]",
    "Show create table",
    arg_type=PARSED_QUERY,
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
    cmd = "exec --no-startup-id rterm-float -e show-sql /tmp/sct_query.sql"
    subprocess.run(
        ["i3-msg", cmd],
        check=True,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    return [(None, None, None, None)]


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
    arg_type=PARSED_QUERY,
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
        return [(None, None, None, "Directed format A: pager=visidata-db, format=csv")]
    elif recipe == "C":
        # Recipe C: no pager with ASCII format
        set_pager_enabled(False)
        list(execute(cur, "\\T ascii"))
        return [(None, None, None, "Directed format C: pager=disabled, format=ascii")]
    else:
        return [(None, None, None, f"Unknown recipe '{recipe}'. Use A or C.")]


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
        "\\gcol",
        "\\dc",
        "\\lt",
        "\\ss",
        "\\sct",
        "\\df",
    )


def reed_suggestions(cmd, arg):
    """Provide context-aware completions for Reed's special commands."""
    if not arg or not arg.strip():
        # No argument yet, suggest tables for most commands
        if cmd == "\\ss":
            # For schema selection, suggest schemas instead of tables
            return [{"type": "schema"}]
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
        elif cmd in ("\\do", "\\du", "\\dd", "\\ddr", "\\dk", "\\tree"):
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
