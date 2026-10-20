"""Provision and verify the isolated Metabase / OrionBelt demo. Fail, never skip."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

import requests

from oracle import CASES, HERE, METRICS, assert_rows, expected, naive_join_revenue

ROOT = HERE.parents[1]
STATE = ROOT / '.logs/orionbelt'
sys.path.insert(0, str(ROOT / 'scripts'))
from agent_smoke import McpClient  # noqa: E402

MB = 'http://127.0.0.1:3002'
OB = 'http://127.0.0.1:18080'
MCP = 'http://127.0.0.1:19000/mcp'
EMAIL, PASSWORD = 'admin@metabase.local', 'Metabase123!'
KNOWN_LIMITATIONS = set()


def wait_http(url, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f'Timed out waiting for {url}')


class Metabase:
    def __init__(self):
        self.http = requests.Session()

    def request(self, method, path, body=None):
        response = self.http.request(method, MB + '/api/' + path, json=body, timeout=120)
        if response.status_code >= 400:
            raise RuntimeError(f'Metabase {method} {path}: HTTP {response.status_code}: {response.text[:600]}')
        return response.json() if response.content else None

    def login(self):
        wait_http(MB + '/api/health')
        props = self.request('GET', 'session/properties')
        token = props.get('setup-token')
        if token:
            session = self.request('POST', 'setup', {
                'token': token, 'prefs': {'site_name': 'One definition, two interfaces',
                                        'site_locale': 'en', 'allow_tracking': False},
                'user': {'email': EMAIL, 'password': PASSWORD, 'first_name': 'Demo', 'last_name': 'Analyst'}})
        else:
            session = self.request('POST', 'session', {'username': EMAIL, 'password': PASSWORD})
        self.http.headers['X-Metabase-Session'] = session['id']

    def query(self, db, sql, columns=None):
        result = self.request('POST', 'dataset', {'database': db, 'type': 'native', 'native': {'query': sql}})
        if result.get('status') != 'completed':
            raise RuntimeError(f'Metabase query failed: {result.get("error")}')
        data = result['data']
        return ordered_rows({'columns': data['cols'], 'rows': data['rows']}, columns) if columns else data['rows']


def semantic_sql(case):
    columns = [*case['dimensions'], *case.get('metrics', METRICS)]
    sql = 'SELECT ' + ', '.join('"' + c + '"' for c in columns) + ' FROM commerce'
    filters = []
    # Only fixture-defined cases enter here. Interactive values use prepared template tags.
    for key, column, op in [('region', 'Region', '='), ('start', 'Order Date', '>='), ('end', 'Order Date', '<=')]:
        if case.get(key):
            filters.append(f'"{column}" {op} ' + "'" + case[key].replace("'", "''") + "'")
    if filters:
        sql += ' WHERE ' + ' AND '.join(filters)
    if case['dimensions']:
        sql += ' ORDER BY ' + ', '.join('"' + d + '"' for d in case['dimensions'])
    return sql


DASHBOARD_CARDS = [
    ('Revenue', [], ['Revenue'], 'scalar', 0, 0, 6, 3),
    ('Refunds', [], ['Refunds'], 'scalar', 0, 6, 6, 3),
    ('Net Revenue', [], ['Net Revenue'], 'scalar', 0, 12, 6, 3),
    ('Customers', [], ['Customer Count'], 'scalar', 0, 18, 6, 3),
    ('Average Order Value', [], ['Average Order Value'], 'scalar', 3, 0, 12, 3),
    ('Refund Rate', [], ['Refund Rate'], 'scalar', 3, 12, 12, 3),
    ('Revenue and refunds by month', ['Month'], ['Revenue', 'Refunds', 'Net Revenue'], 'bar', 6, 0, 12, 7),
    ('Net revenue by region', ['Region'], ['Net Revenue'], 'bar', 6, 12, 12, 7),
    ('Inspect the shared metrics', ['Month', 'Region'], METRICS, 'table', 13, 0, 24, 7),
]


def dashboard():
    state = json.loads((STATE / 'state.json').read_text())
    mb = Metabase()
    mb.login()
    collection_name = 'OrionBelt | Shared semantics'
    collections = mb.request('GET', 'collection')
    collection = next((c for c in collections if c['name'] == collection_name), None)
    if not collection:
        collection = mb.request('POST', 'collection', {'name': collection_name,
            'description': 'One semantic model, Metabase and MCP, independently verified.'})
    collection_id = collection['id']
    parameters = [
        {'id': 'region', 'slug': 'region', 'name': 'Region', 'type': 'string/=', 'sectionId': 'string'},
        {'id': 'start', 'slug': 'start', 'name': 'From order date', 'type': 'date/single', 'sectionId': 'date'},
        {'id': 'end', 'slug': 'end', 'name': 'Through order date', 'type': 'date/single', 'sectionId': 'date'},
    ]
    title = 'One definition, two interfaces'
    dashboards = mb.request('GET', 'dashboard')
    existing = next((d for d in dashboards if d['name'] == title and d.get('collection_id') == collection_id), None)
    body = {'name': title, 'collection_id': collection_id, 'parameters': parameters,
            'description': 'Synthetic commerce in EUR. Refunds follow the original order date. '
            'All charts use OrionBelt definitions also available to AI through MCP. '
            'Run demo.py verify for independent evidence; agreement alone is insufficient.'}
    board = mb.request('PUT', f'dashboard/{existing["id"]}', body) if existing else mb.request('POST', 'dashboard', body)
    cards = mb.request('GET', 'card')
    dashcards, saved = [], []
    for index, (name, dimensions, metrics, display, row, col, width, height) in enumerate(DASHBOARD_CARDS):
        case = {'dimensions': dimensions, 'metrics': metrics, 'start': '2026-01-01'}
        query = semantic_sql(case)
        order = ''
        if ' ORDER BY ' in query:
            query, order = query.split(' ORDER BY ')
            order = ' ORDER BY ' + order
        query += ('\n[[AND "Region" = {{region}}]]'
                  '\n[[AND "Order Date" >= {{start}}]]'
                  '\n[[AND "Order Date" <= {{end}}]]') + order
        tags = {p['id']: {'id': p['id'], 'name': p['id'], 'display-name': p['name'],
                         'type': 'text' if p['id'] == 'region' else 'date'} for p in parameters}
        settings = {}
        if display == 'bar':
            settings = {'graph.dimensions': dimensions, 'graph.metrics': metrics,
                        'graph.colors': ['#509EE3', '#ED6E6E', '#88BF4D']}
        elif display == 'scalar':
            settings = {'scalar.decimals': 0 if name == 'Customers' else 2}
            if name == 'Refund Rate':
                settings.update({'scalar.scale': 100, 'scalar.suffix': '%'})
            elif name != 'Customers':
                settings['scalar.prefix'] = '€'
        card_body = {'name': name, 'collection_id': collection_id, 'display': display,
                     'description': 'Definition: commerce.yaml. Refunds use order date. Amounts in EUR.',
                     'visualization_settings': settings,
                     'dataset_query': {'type': 'native', 'database': state['database_id'],
                                       'native': {'query': query, 'template-tags': tags}}}
        existing = next((c for c in cards if c['name'] == name and c.get('collection_id') == collection_id), None)
        card = mb.request('PUT', f'card/{existing["id"]}', card_body) if existing else mb.request('POST', 'card', card_body)
        dashcards.append({'id': -(index + 1), 'card_id': card['id'], 'row': row, 'col': col,
                          'size_x': width, 'size_y': height, 'visualization_settings': settings,
                          'parameter_mappings': [{'parameter_id': p['id'], 'card_id': card['id'],
                              'target': ['variable', ['template-tag', p['id']]]} for p in parameters]})
        saved.append({'id': card['id'], 'name': name, 'dimensions': dimensions, 'metrics': metrics})
    mb.request('PUT', f'dashboard/{board["id"]}', {'dashcards': dashcards})
    state.update({'dashboard_id': board['id'], 'cards': saved})
    (STATE / 'state.json').write_text(json.dumps(state, indent=2), encoding='utf-8')
    print(f'Dashboard: {MB}/dashboard/{board["id"]}', flush=True)


def semantic_query(case):
    query = {'select': {'dimensions': case['dimensions'], 'measures': case.get('metrics', METRICS)}, 'limit': 1000}
    filters = []
    for key, field, op in [('region', 'Region', 'equals'), ('start', 'Order Date', 'gte'), ('end', 'Order Date', 'lte')]:
        if case.get(key):
            filters.append({'field': field, 'op': op, 'value': case[key]})
    if filters:
        query['where'] = filters
    if case['dimensions']:
        query['orderBy'] = [{'field': d, 'direction': 'asc'} for d in case['dimensions']]
    return query


def mcp_connect(url, key=''):
    client = McpClient(MB, key)
    client.url = url
    if not key:
        client.session.headers.pop('x-api-key', None)
    deadline = time.monotonic() + 120
    while True:
        try:
            client.initialize()
            break
        except requests.ConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)
    return client


def tool(client, name, arguments):
    result = client.call('tools/call', {'name': name, 'arguments': arguments})
    if result.get('isError'):
        raise RuntimeError(f'MCP {name}: {result}')
    return result


def tool_json(result):
    structured = result.get('structuredContent')
    if structured and 'rows' in structured:
        return structured
    for content in result.get('content', []):
        if content.get('type') == 'text':
            try:
                parsed = json.loads(content['text'])
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
    raise RuntimeError(f'MCP did not return a JSON result: {result}')


def ordered_rows(result, columns):
    """REST puts base measures before metrics; match named columns, never positions."""
    metadata = result.get('columns', result.get('cols'))
    if metadata is None or result.get('truncated'):
        raise AssertionError('Missing column metadata or truncated result')
    actual = [c['name'] if isinstance(c, dict) else c for c in metadata]
    if any(len(row) != len(actual) for row in result['rows']):
        raise AssertionError('Result row width differs from column metadata')
    if result['rows'] == [] and actual == columns + [c + '_2' for c in columns]:
        # Reproduced directly with Arrow JDBC 19 (outside Metabase): zero-row
        # queries expose duplicated metadata. Validate the exact known pattern
        # and empty row set, retain it in evidence, never normalize nonempty data.
        KNOWN_LIMITATIONS.add('Arrow JDBC 19 repeats column metadata for empty results; '
                              'empty-result row sets pass, metadata parity does not.')
        return []
    if len(actual) != len(set(actual)) or set(actual) != set(columns):
        raise AssertionError(f'Unexpected result columns: {actual}; expected {columns}')
    indices = [actual.index(c) for c in columns]
    return [[row[index] for index in indices] for row in result['rows']]


def provision():
    wait_http(OB + '/health')
    mb = Metabase()
    mb.login()
    STATE.mkdir(parents=True, exist_ok=True)
    databases = mb.request('GET', 'database')['data']
    db = next((d for d in databases if d['name'] == 'OrionBelt Commerce'), None)
    if not db:
        db = mb.request('POST', 'database', {'name': 'OrionBelt Commerce', 'engine': 'arrow-flight-sql',
            'details': {'host': 'orionbelt', 'port': 8815, 'useEncryption': False,
                        'use-token': False, 'enable-uploads': False, 'enable-actions': False},
            'is_full_sync': False, 'auto_run_queries': False})
    db_id = db['id']
    mb.request('POST', f'database/{db_id}/sync_schema')
    deadline = time.monotonic() + 120
    metadata = None
    while time.monotonic() < deadline:
        metadata = mb.request('GET', f'database/{db_id}/metadata')
        if any(t['name'] == 'model' and t['schema'] == 'commerce'
               and set(METRICS + ['Region', 'Month', 'Order Date']) <= {f['name'] for f in t.get('fields', [])}
               for t in metadata.get('tables', [])):
            break
        time.sleep(2)
    else:
        raise RuntimeError('Flight SQL metadata did not synchronize')
    (STATE / 'metadata.json').write_text(json.dumps(metadata, indent=2), encoding='utf-8')
    print(f'Metabase connection {db_id}; {len(metadata["tables"])} tables discovered', flush=True)
    # A persisted per-demo key is separate from the repository's other stacks.
    key_path = STATE / 'api-key'
    valid_key = (key_path.exists() and requests.get(MB + '/api/user/current',
        headers={'x-api-key': key_path.read_text().strip()}, timeout=15).status_code == 200)
    if not valid_key:
        groups = mb.request('GET', 'permissions/group')
        group = next(g['id'] for g in groups if g['name'] == 'Administrators')
        key = mb.request('POST', 'api-key', {'name': 'orionbelt-demo', 'group_id': group})['unmasked_key']
        key_path.write_text(key, encoding='utf-8')
    state_path = STATE / 'state.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    state['database_id'] = db_id
    (STATE / 'state.json').write_text(json.dumps(state, indent=2), encoding='utf-8')
    print('Provisioned connection; next: verify and dashboard', flush=True)


def verify_paths(report):
    state = json.loads((STATE / 'state.json').read_text())
    db = state['database_id']
    mb = Metabase()
    mb.login()
    ob_mcp = mcp_connect(MCP)
    tools = ob_mcp.call('tools/list', {})['tools']
    if 'execute_query' not in {t['name'] for t in tools}:
        raise RuntimeError('OrionBelt MCP is not serving the loaded model')
    description = tool(ob_mcp, 'describe_model', {})
    (STATE / 'mcp-model.json').write_text(json.dumps(description, indent=2), encoding='utf-8')
    explanation = tool(ob_mcp, 'explain_artefact', {'name': 'Net Revenue'})
    lineage = tool(ob_mcp, 'get_lineage', {'name': 'Net Revenue', 'output_format': 'json'})
    report['semantic_discovery'] = {'explanation': explanation, 'lineage': lineage}
    for name in ('Revenue', 'Refund Amount'):
        if name not in json.dumps(explanation) or name not in json.dumps(lineage):
            raise AssertionError('MCP lineage did not expose the Net Revenue dependencies')
    mb_mcp = mcp_connect(MB + '/api/metabase-mcp', (STATE / 'api-key').read_text().strip())
    if naive_join_revenue() == expected(CASES[0])[0][0]:
        raise AssertionError('Negative control does not expose join inflation')
    cases = [dict(case, metrics=metrics) for case in CASES for metrics in [METRICS, *[[m] for m in METRICS]]]
    for case in cases:
        metrics = case['metrics']
        all_columns = [*case['dimensions'], *METRICS]
        columns = [*case['dimensions'], *metrics]
        indices = [all_columns.index(c) for c in columns]
        wanted = [[row[i] for i in indices] for row in expected(case)]
        sql = semantic_sql(case)
        mb_rows = mb.query(db, sql, columns)
        assert_rows(mb_rows, wanted, case['name'] + '/metabase', columns)
        ai = tool_json(tool(ob_mcp, 'execute_query', {'query_json': json.dumps(semantic_query(case)),
                                                     'format_values': False}))
        ai_rows = ordered_rows(ai, columns)
        assert_rows(ai_rows, wanted, case['name'] + '/orionbelt-mcp', columns)
        via_mb = tool_json(tool(mb_mcp, 'execute_sql', {'database_id': db, 'sql': sql,
            'template_tag_values': None, 'prompt': 'Verify the shared commerce semantic model: ' + case['name'],
            'validate_only': False, 'row_limit': 1000}))
        via_mb_rows = ordered_rows(via_mb, columns)
        assert_rows(via_mb_rows, wanted, case['name'] + '/metabase-mcp', columns)
        report['cases'].append({'name': case['name'], 'metrics': metrics, 'sql': sql, 'query_object': semantic_query(case),
            'expected': wanted, 'metabase': mb_rows, 'orionbelt_mcp': ai_rows,
            'metabase_mcp': via_mb_rows, 'compiled_sql': ai.get('sql'),
            'planner': ai.get('explain'), 'warnings': ai.get('warnings'), 'passed': True})
        if metrics == METRICS:
            print('PASS ' + case['name'] + ': all three query paths + independent oracle', flush=True)
    print(f'PASS {len(cases)} query shapes across three paths (including each metric alone)', flush=True)
    verify_dashboard(mb, state, report)


def verify_dashboard(mb, state, report):
    board = mb.request('GET', f'dashboard/{state["dashboard_id"]}')
    if len(board['dashcards']) != len(DASHBOARD_CARDS):
        raise AssertionError('Dashboard provisioning duplicated or omitted cards')
    report['dashboard'] = []
    scenarios = [CASES[i] for i in [0, 4, 5, 7, 8]]
    for case in scenarios:
        for card in state['cards']:
            dc = next(d for d in board['dashcards'] if d.get('card_id') == card['id'])
            params = []
            for mapping in dc['parameter_mappings']:
                key = mapping['parameter_id']
                if key in case:
                    definition = next(p for p in board['parameters'] if p['id'] == key)
                    params.append({'id': key, 'type': definition['type'], 'value': case[key],
                                   'target': mapping['target']})
            result = mb.request('POST', f'card/{card["id"]}/query', {'parameters': params, 'ignore_cache': True})
            if result.get('status') != 'completed':
                raise RuntimeError(f'Dashboard card {card["name"]}: {result.get("error")}')
            columns = [*card['dimensions'], *card['metrics']]
            all_columns = [*card['dimensions'], *METRICS]
            indices = [all_columns.index(c) for c in columns]
            wanted = [[r[i] for i in indices] for r in expected(dict(case, dimensions=card['dimensions']))]
            rows = ordered_rows(result['data'], columns)
            assert_rows(rows, wanted, f'dashboard/{case["name"]}/{card["name"]}', columns)
            report['dashboard'].append({'scenario': case['name'], 'card': card['name'],
                                       'parameters': params, 'rows': rows, 'expected': wanted, 'passed': True})
        print('PASS all 9 saved cards with dashboard mappings: ' + case['name'], flush=True)


def verify():
    KNOWN_LIMITATIONS.clear()
    STATE.mkdir(parents=True, exist_ok=True)
    report = {'timestamp': datetime.now(timezone.utc).isoformat(), 'status': 'running',
              'model_sha256': hashlib.sha256((HERE / 'commerce.yaml').read_bytes()).hexdigest(),
              'fixture_sha256': hashlib.sha256((HERE / 'fixture.json').read_bytes()).hexdigest(),
              'naive_join_revenue': str(naive_join_revenue()), 'cases': []}
    path = STATE / 'verification.json'
    # Invalidate previous success before any network operation, even if interrupted.
    path.write_text(json.dumps(report, indent=2), encoding='utf-8')
    try:
        verify_paths(report)
        report['status'] = 'passed_with_known_limitations' if KNOWN_LIMITATIONS else 'passed'
    except Exception as error:
        report.update(status='failed', error=str(error))
        raise
    finally:
        report['known_limitations'] = sorted(KNOWN_LIMITATIONS)
        path.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
    print('Evidence: ' + str(STATE / 'verification.json'), flush=True)
    for limitation in sorted(KNOWN_LIMITATIONS):
        print('KNOWN LIMITATION: ' + limitation, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['provision', 'dashboard', 'verify'])
    args = parser.parse_args()
    {'provision': provision, 'dashboard': dashboard, 'verify': verify}[args.command]()
