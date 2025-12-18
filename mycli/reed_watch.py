import datetime as dt
import re
from time import sleep, time

import click

DEFAULT_WATCH_SECONDS = 2


def get_watch_command(command):
    """Parse command for \\watch at the end.

    Returns (query, timing) tuple if \\watch found, otherwise (None, None).
    Syntax: SELECT * FROM table \\watch 5
    """
    match = re.match(r"(.*?)[\s]*\\watch(\s+\d+)?\s*;?\s*$", command, re.DOTALL)
    if match:
        groups = match.groups(default=f"{DEFAULT_WATCH_SECONDS}")
        return groups[0], int(groups[1])
    return None, None


def handle_watch_command(mycli, text):
    """Handle \\watch command. Returns True if watch was handled, False otherwise."""
    watch_command, timing = get_watch_command(text)

    if watch_command is not None and not watch_command.strip():
        try:
            watch_command = mycli.query_history[-1].query
        except IndexError:
            click.secho("\\watch cannot be used with an empty query", err=True, fg="red")
            return True

    if watch_command:
        try:
            _run_watch_loop(mycli, watch_command, timing)
        except KeyboardInterrupt:
            pass
        return True

    return False


def _run_watch_loop(mycli, watch_command, timing):
    """Run the watch loop with enhanced display features."""
    last_data = None
    sqlexecute = mycli.sqlexecute
    old_format = mycli.main_formatter.format_name

    try:
        while True:
            start = time()
            try:
                res = sqlexecute.run(watch_command)

                results = []
                current_data = []
                for title, cur, headers, status in res:
                    if cur:
                        rows = list(cur)
                    else:
                        rows = None
                    results.append((title, rows, headers, status))
                    current_data.append((rows, headers))

                execution_time = time() - start
                timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                status_line = click.style(f"Last run: {timestamp}", fg="bright_black")

                if current_data == last_data:
                    click.echo(f"\033[F\033[2K{status_line}")
                else:
                    click.clear()
                    mycli.main_formatter.format_name = "ascii"
                    for title, rows, headers, status in results:
                        if rows is not None:
                            output = mycli.format_output(
                                title,
                                rows,
                                headers,
                                expanded=False,
                                is_redirected=False,
                                max_width=None,
                            )
                            click.echo("\n".join(output))
                        elif status:
                            click.echo(status)
                    click.echo(f"Time: {execution_time:.3f}s")
                    click.echo(status_line)
                    last_data = current_data

                sleep(timing)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                click.secho(str(e), err=True, fg="red")
                break
    finally:
        mycli.main_formatter.format_name = old_format
