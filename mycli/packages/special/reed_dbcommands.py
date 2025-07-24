import logging
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
    cols = find_useful_columns(cur, table)
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
    cols = find_useful_columns(cur, table)
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
    cols = find_useful_columns(cur, table)
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
    cols = find_useful_columns(cur, table)
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


def find_useful_columns(cur, table):
    query = f"show fields from {table}"
    log.debug(query)
    cur.execute(query)
    columns = [x[0] for x in cur.fetchall()]
    usefuls = set(["id", "parent_id", "level", "kode", "code", "nama", "name"])
    return [x for x in columns if x in usefuls]


def is_reed_command(cmd):
    """Check if a command is one of Reed's special commands."""
    return cmd in ("\\d", "\\do", "\\dd", "\\du", "\\ddr", "\\dk", "\\tree", "\\gcol", "\\dc", "\\ss", "\\sct")


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
    
    return []
