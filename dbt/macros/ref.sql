{#
  ref() without the database, so a view names `schema.model`, not
  `catalog.schema.model`.

  DuckDB names a database's catalog after its file, and scripts/run_dbt.py
  builds the mart as retail_candidate_<id>.duckdb and renames it to
  retail_<id>.duckdb on promotion. A view stores the SQL it was created with,
  so a ref() compiled with the catalog kept `retail_candidate_<id>` inside
  every view built on another model, and in the promoted file
  int_sales_measures failed with 'Catalog "retail_candidate_<id>" does not
  exist'. The marts are tables, so every data test still passed.

  `schema.model` resolves in the catalog the view lives in, whatever the file
  is called and whatever name a reader ATTACHes it under. Checked by
  tests/test_dbt_wrapper.py, which queries every view in the promoted mart.
#}
{% macro ref() %}
    {% do return(builtins.ref(*varargs, **kwargs).include(database=False)) %}
{% endmacro %}
