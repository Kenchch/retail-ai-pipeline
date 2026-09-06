import sqlite3

import pandas as pd

from retail_pipeline.pipeline import _stage_tables, _swap_tables


def test_quoted_table_names_survive_publication_without_affecting_other_tables():
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE protected (id INTEGER)")
        name = 'a"; DROP TABLE protected; --'
        tables = {name: pd.DataFrame({"value": [42]})}
        _stage_tables(conn, tables)
        _swap_tables(conn, tables)
        quoted = '"' + name.replace('"', '""') + '"'
        assert conn.execute(f"SELECT value FROM {quoted}").fetchall() == [(42,)]
        assert conn.execute("SELECT count(*) FROM protected").fetchone() == (0,)
