"""Demo-local OrionBelt 2.34.0 fixes: numeric probes, catalog SQL, result order.

See COMPATIBILITY.md before upgrading/removing these version-specific adapters.
Business formulas and query planning remain in the semantic model/compiler.
"""
from decimal import Decimal

import pyarrow as pa
import duckdb
from ob_flight import server_catalog, server_execution
import sqlglot
import sqlglot.expressions as exp

original_probe = server_catalog.catalog_scalar_probe_table
original_dispatch = server_catalog._dispatch_catalog_sql
original_classify = server_execution.classify_sql
original_align = server_execution.align_cached_table


def align_by_name(table, schema):
    """Match streamed values to advertised Flight column names before casting.

    The compiler groups base measures before derived metrics, whereas the
    advertised schema follows the requested order. Stock 2.34.0 streams the
    mismatched order unchanged; JDBC then labels values with the wrong metric.
    """
    if schema is None:
        return table
    if (len(set(table.column_names)) != len(table.column_names)
            or len(set(schema.names)) != len(schema.names)
            or set(table.column_names) != set(schema.names)):
        raise ValueError(f'Flight result columns {table.column_names} differ from advertised {schema.names}')
    aligned = original_align(table.select(schema.names), schema)
    if not aligned.schema.equals(schema):
        raise ValueError('Flight result types differ from the advertised semantic schema')
    return aligned


def classify_quoted_catalog(server, sql, model):
    # The stock classifier also string-matches `information_schema.` and
    # misses a quoted schema. Do not change semantic SQL or multi-statements.
    try:
        statements = sqlglot.parse(sql, read='postgres')
    except sqlglot.errors.ParseError:
        return original_classify(server, sql, model)
    if len(statements) == 1 and isinstance(statements[0], exp.Select):
        ast = statements[0]
        changed = False
        for table in ast.find_all(exp.Table):
            if table.db.lower() in {'information_schema', 'pg_catalog'}:
                table.args['db'].set('quoted', False)
                changed = True
        if changed:
            sql = ast.sql(dialect='postgres')
    return original_classify(server, sql, model)


def quoted_catalog_dispatch(server, sql, model, *, project=True):
    """Expose SQL information_schema names and evaluate the metadata SELECT.

    2.34.0 returns Flight protocol names (db_schema_name) to SQL clients and
    ignores expression projections such as UPPER(data_type). Use only the
    in-memory, model-derived catalog. The warehouse is never opened here.
    External access is disabled and exactly one known catalog table is allowed.
    """
    try:
        statements = sqlglot.parse(sql, read='postgres')
    except sqlglot.errors.ParseError:
        return original_dispatch(server, sql, model, project=project)
    if len(statements) != 1 or statements[0] is None:
        return original_dispatch(server, sql, model, project=project)
    ast = statements[0]
    tables = list(ast.find_all(exp.Table))
    if (isinstance(ast, exp.Select) and len(tables) == 1
            and tables[0].db.lower() == 'information_schema'
            and tables[0].name.lower() in {'tables', 'columns'}
            and not any(ast.find_all(exp.Join, exp.Subquery, exp.CTE))):
        target = tables[0]
        source = (server_catalog.catalog_tables_table(model) if target.name.lower() == 'tables'
                  else server_catalog.catalog_columns_table(model))
        source = source.rename_columns([{'catalog_name': 'table_catalog',
                                         'db_schema_name': 'table_schema'}.get(n, n)
                                        for n in source.column_names])
        target.set('db', None)
        target.set('catalog', None)
        target.set('this', exp.to_identifier('ob_catalog'))
        if not project:
            ast.set('expressions', [exp.Star()])
        with duckdb.connect(':memory:', config={'enable_external_access': 'false', 'threads': '1'}) as conn:
            conn.register('ob_catalog', source)
            return conn.execute(ast.sql(dialect='duckdb')).fetch_arrow_table(), True
    return original_dispatch(server, sql, model, project=project)


def typed_scalar_probe(ast):
    table = original_probe(ast)
    for index, projection in enumerate(ast.expressions):
        inner = projection.this if isinstance(projection, exp.Alias) else projection
        if isinstance(inner, exp.Literal) and not inner.is_string:
            literal = str(inner.this)
            value = Decimal(literal) if any(c in literal.lower() for c in '.e') else int(literal)
            table = table.set_column(index, table.column_names[index], pa.array([value]))
    return table


def main():
    server_catalog.catalog_scalar_probe_table = typed_scalar_probe
    server_catalog._dispatch_catalog_sql = quoted_catalog_dispatch
    server_execution.classify_sql = classify_quoted_catalog
    server_execution.align_cached_table = align_by_name
    import uvicorn
    uvicorn.run('orionbelt.api.app:create_app', factory=True, host='0.0.0.0', port=8080,
                access_log=False)


if __name__ == '__main__':
    main()
