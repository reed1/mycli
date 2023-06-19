import logging
import re
from .main import special_command, PARSED_QUERY

log = logging.getLogger(__name__)


@special_command('\\du', '\\du [table] [id]', 'Drill up row', arg_type=PARSED_QUERY, case_sensitive=True)
def drill_up(cur, arg=None, **_):
    [table, row_id, *args] = re.split(r'\s+', arg)
    cols = find_useful_columns(cur, table)
    q_cols = ', '.join(cols)
    qr_cols = ', '.join([f'r.{x}' for x in cols])
    query = f"""
    with recursive cte as (
        select {q_cols}, 1 as depth from {table} where id = {row_id}
        union all
        select {qr_cols}, cte.depth + 1 from {table} as t
        inner join cte on t.id = cte.parent_id
    )
    select {q_cols} from cte {' '.join(args)} order by depth desc
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [(None, cur, headers, '')]
    else:
        return [(None, None, None, '')]


@special_command('\\dd', '\\dd [table] [id]', 'Drill down row', arg_type=PARSED_QUERY, case_sensitive=True)
def drill_down(cur, arg=None, **_):
    [table, row_id, *args] = re.split(r'\s+', arg)
    cols = find_useful_columns(cur, table)
    q_cols = ', '.join(cols)
    query = f"""
    select * from (
        select {q_cols} from {table} where parent_id = {row_id}
    ) as t
    {' '.join(args)}
    """
    log.debug(query)
    cur.execute(query)
    if cur.description:
        headers = [x[0] for x in cur.description]
        return [(None, cur, headers, '')]
    else:
        return [(None, None, None, '')]


def find_useful_columns(cur, table):
    query = f'SHOW FIELDS FROM {table}'
    log.debug(query)
    cur.execute(query)
    columns = [x[0] for x in cur.fetchall()]
    usefuls = set([
        'id', 'parent_id', 'level', 'kode', 'code', 'nama', 'name'
    ])
    return [x for x in columns if x in usefuls]
