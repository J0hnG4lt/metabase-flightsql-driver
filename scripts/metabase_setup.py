#!/usr/bin/env python3
"""
Metabase Automated Setup and Testing Script

This script automates:
1. Initial Metabase setup (admin user creation)
2. Database connection creation (GizmoSQL and Spice)
3. API key generation
4. Comprehensive dashboard creation with field filters and multiple chart types
5. Both native SQL and GUI-based queries for driver stress testing
"""

import os
import sys
import json
import time
import requests
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class MetabaseConfig:
    """Configuration for Metabase connection"""
    base_url: str = field(default_factory=lambda: os.environ.get("METABASE_URL", "http://localhost:3000").rstrip("/"))
    admin_email: str = "admin@metabase.local"
    admin_password: str = "Metabase123!"
    admin_first_name: str = "Admin"
    admin_last_name: str = "User"
    site_name: str = "Metabase FlightSQL Test"


class MetabaseClient:
    """Client for interacting with Metabase API"""

    def __init__(self, config: MetabaseConfig):
        self.config = config
        self.session = requests.Session()
        self.session_token: Optional[str] = None
        self.api_key: Optional[str] = None
        self._table_cache: Dict[int, Dict] = {}
        self._field_cache: Dict[int, Dict] = {}

    def _headers(self) -> Dict[str, str]:
        """Get headers for API requests"""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key
        elif self.session_token:
            headers["X-Metabase-Session"] = self.session_token
        return headers

    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None,
                 expected_status: Optional[int] = None) -> Dict:
        """Make an API request"""
        url = f"{self.config.base_url}/api/{endpoint}"
        try:
            if method == "GET":
                response = self.session.get(url, headers=self._headers())
            elif method == "POST":
                response = self.session.post(url, headers=self._headers(), json=data)
            elif method == "PUT":
                response = self.session.put(url, headers=self._headers(), json=data)
            elif method == "DELETE":
                response = self.session.delete(url, headers=self._headers())
            else:
                raise ValueError(f"Unknown method: {method}")

            success_codes = [200, 201, 202]
            if expected_status:
                success_codes = [expected_status]

            if response.status_code not in success_codes and response.status_code >= 400:
                print(f"Warning: {method} {endpoint} returned {response.status_code}")
                print(f"Response: {response.text[:500]}")

            if response.text:
                content_type = response.headers.get('content-type', '')
                if content_type.startswith('application/json'):
                    return response.json()
                else:
                    return {"text": response.text, "status_code": response.status_code}
            return {"status_code": response.status_code}
        except requests.exceptions.ConnectionError:
            print(f"Error: Cannot connect to Metabase at {self.config.base_url}")
            return {"error": "Connection error"}
        except json.JSONDecodeError:
            return {"text": response.text, "status_code": response.status_code}

    # ==================== Health & Status ====================

    def wait_for_ready(self, timeout: int = 120) -> bool:
        """Wait for Metabase to be ready"""
        print("Waiting for Metabase to be ready...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                response = self.session.get(f"{self.config.base_url}/api/health")
                if response.status_code == 200:
                    print("Metabase is ready!")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(2)
        print("Timeout waiting for Metabase")
        return False

    def get_session_properties(self) -> Dict:
        """Get session properties including setup token"""
        return self._request("GET", "session/properties")

    def is_setup_complete(self) -> bool:
        """Check if initial setup is complete"""
        props = self.get_session_properties()
        return props.get("has-user-setup", False) and not props.get("setup-token")

    def get_setup_token(self) -> Optional[str]:
        """Get the setup token for initial configuration"""
        props = self.get_session_properties()
        return props.get("setup-token")

    # ==================== Setup & Authentication ====================

    def setup(self) -> bool:
        """Perform initial Metabase setup"""
        token = self.get_setup_token()
        if not token:
            print("No setup token available - setup may already be complete")
            return False

        print("Performing initial setup")

        setup_data = {
            "token": token,
            "prefs": {
                "site_name": self.config.site_name,
                "site_locale": "en",
                "allow_tracking": False
            },
            "user": {
                "first_name": self.config.admin_first_name,
                "last_name": self.config.admin_last_name,
                "email": self.config.admin_email,
                "password": self.config.admin_password
            }
        }

        result = self._request("POST", "setup", setup_data)
        if result.get("id"):
            self.session_token = result["id"]
            print("Setup complete")
            return True
        print(f"Setup failed: {result}")
        return False

    def login(self) -> bool:
        """Login with admin credentials"""
        print(f"Logging in as {self.config.admin_email}...")
        result = self._request("POST", "session", {
            "username": self.config.admin_email,
            "password": self.config.admin_password
        })
        if result.get("id"):
            self.session_token = result["id"]
            print(f"Login successful! Session: {self.session_token[:8]}...")
            return True
        print(f"Login failed: {result}")
        return False

    def set_api_key(self, api_key: str):
        """Set API key for authentication"""
        self.api_key = api_key
        self.session_token = None

    # ==================== API Key Management ====================

    def get_api_keys(self) -> List[Dict]:
        """Get list of existing API keys"""
        return self._request("GET", "api-key")

    def create_api_key(self, name: str = "automation-key") -> Optional[str]:
        """Create a new API key"""
        print(f"Creating API key: {name}...")

        groups = self._request("GET", "permissions/group")
        admin_group = next((g for g in groups if g.get("name") == "Administrators"), None)
        if not admin_group:
            print("Could not find Administrators group")
            return None

        result = self._request("POST", "api-key", {
            "name": name,
            "group_id": admin_group["id"]
        })

        if result.get("unmasked_key"):
            key = result["unmasked_key"]
            print("API key created")
            return key
        print(f"Failed to create API key: {result}")
        return None

    # ==================== Database Management ====================

    def get_databases(self) -> List[Dict]:
        """Get list of databases"""
        return self._request("GET", "database")

    def find_database(self, name: str) -> Optional[Dict]:
        """Find a database by name"""
        databases = self.get_databases()
        if isinstance(databases, dict) and "data" in databases:
            databases = databases["data"]
        for db in databases:
            if db.get("name") == name:
                return db
        return None

    def create_database(self, name: str, engine: str, details: Dict) -> Optional[Dict]:
        """Create a new database connection"""
        print(f"Creating database: {name} ({engine})...")

        existing = self.find_database(name)
        if existing:
            print(f"Database '{name}' already exists (ID: {existing['id']})")
            return existing

        result = self._request("POST", "database", {
            "name": name,
            "engine": engine,
            "details": details,
            "auto_run_queries": True,
            "is_full_sync": True,
            "is_on_demand": False,
            "schedules": {}
        })

        if result.get("id"):
            print(f"Database created! ID: {result['id']}")
            return result
        print(f"Failed to create database: {result}")
        return None

    def sync_database(self, db_id: int) -> bool:
        """Trigger database schema sync"""
        print(f"Syncing database {db_id}...")
        result = self._request("POST", f"database/{db_id}/sync_schema")
        return result.get("status") == "ok"

    def get_database_metadata(self, db_id: int) -> Dict:
        """Get database metadata including tables and fields"""
        return self._request("GET", f"database/{db_id}/metadata")

    def cache_database_fields(self, db_id: int):
        """Cache all field IDs for a database"""
        if db_id in self._field_cache:
            return

        meta = self.get_database_metadata(db_id)
        self._field_cache[db_id] = {}
        self._table_cache[db_id] = {}
        for table in meta.get("tables", []):
            schema = table.get("schema", "")
            table_name = table.get("name")
            table_key = f"{schema}.{table_name}"
            self._table_cache[db_id][table_key] = table
            for field in table.get("fields", []):
                key = f"{schema}.{table_name}.{field.get('name')}"
                self._field_cache[db_id][key] = {
                    "id": field.get("id"),
                    "name": field.get("name"),
                    "base_type": field.get("base_type"),
                    "semantic_type": field.get("semantic_type"),
                    "table_id": table.get("id"),
                    "table_name": table_name,
                    "schema": schema
                }

    def get_field_id(self, db_id: int, schema: str, table: str, field: str) -> Optional[int]:
        """Get field ID by schema.table.field path"""
        self.cache_database_fields(db_id)
        key = f"{schema}.{table}.{field}"
        field_info = self._field_cache.get(db_id, {}).get(key)
        return field_info.get("id") if field_info else None

    def get_field_info(self, db_id: int, schema: str, table: str, field: str) -> Optional[Dict]:
        """Get full field info by schema.table.field path"""
        self.cache_database_fields(db_id)
        key = f"{schema}.{table}.{field}"
        return self._field_cache.get(db_id, {}).get(key)

    def get_table_info(self, db_id: int, schema: str, table: str) -> Optional[Dict]:
        """Get table info by schema.table path"""
        self.cache_database_fields(db_id)
        key = f"{schema}.{table}"
        return self._table_cache.get(db_id, {}).get(key)

    # ==================== Predefined Database Configs ====================

    def create_gizmosql_connection(self, host: str = "gizmosql", port: int = 31337,
                                    username: str = "gizmosql",
                                    password: str = "gizmosql_password",
                                    catalog: str = "memory") -> Optional[Dict]:
        """Create GizmoSQL database connection"""
        return self.create_database(
            name="gizmo",
            engine="arrow-flight-sql",
            details={
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "catalog": catalog,
                "useEncryption": False,
                "disableCertificateVerification": True
            }
        )

    def create_spice_connection(self, host: str = "spiced-container",
                                 port: int = 50051,
                                 token: str = "1234567890") -> Optional[Dict]:
        """Create Spice database connection"""
        return self.create_database(
            name="flight",
            engine="arrow-flight-sql",
            details={
                "host": host,
                "port": port,
                "token": token,
                "useEncryption": False,
                "disableCertificateVerification": True
            }
        )

    # ==================== Card/Question Management ====================

    def create_native_card(self, name: str, database_id: int, query: str,
                           collection_id: Optional[int] = None,
                           display: str = "table",
                           visualization_settings: Optional[Dict] = None,
                           template_tags: Optional[Dict] = None) -> Optional[Dict]:
        """Create a native SQL card/question with optional template tags for filters"""
        print(f"Creating native card: {name} ({display})...")

        native_query = {"query": query}
        if template_tags:
            native_query["template-tags"] = template_tags

        card_data = {
            "name": name,
            "display": display,
            "dataset_query": {
                "database": database_id,
                "type": "native",
                "native": native_query
            },
            "visualization_settings": visualization_settings or {}
        }
        if collection_id:
            card_data["collection_id"] = collection_id

        result = self._request("POST", "card", card_data)
        if result.get("id"):
            print(f"Card created! ID: {result['id']}")
            return result
        print(f"Failed to create card: {result}")
        return None

    def create_mbql_card(self, name: str, database_id: int, table_id: int,
                         aggregations: Optional[List] = None,
                         breakouts: Optional[List] = None,
                         filters: Optional[List] = None,
                         order_by: Optional[List] = None,
                         limit: Optional[int] = None,
                         collection_id: Optional[int] = None,
                         display: str = "table",
                         visualization_settings: Optional[Dict] = None) -> Optional[Dict]:
        """Create an MBQL (GUI-based) card/question"""
        print(f"Creating MBQL card: {name} ({display})...")

        query = {"source-table": table_id}
        if aggregations:
            query["aggregation"] = aggregations
        if breakouts:
            query["breakout"] = breakouts
        if filters:
            query["filter"] = filters
        if order_by:
            query["order-by"] = order_by
        if limit:
            query["limit"] = limit

        card_data = {
            "name": name,
            "display": display,
            "dataset_query": {
                "database": database_id,
                "type": "query",
                "query": query
            },
            "visualization_settings": visualization_settings or {}
        }
        if collection_id:
            card_data["collection_id"] = collection_id

        result = self._request("POST", "card", card_data)
        if result.get("id"):
            print(f"Card created! ID: {result['id']}")
            return result
        print(f"Failed to create card: {result}")
        return None

    def get_cards(self) -> List[Dict]:
        """Get list of cards/questions"""
        return self._request("GET", "card")

    def run_card(self, card_id: int) -> Dict:
        """Run a card and get results"""
        return self._request("POST", f"card/{card_id}/query")

    # ==================== Dashboard Management ====================

    def create_dashboard(self, name: str, description: str = "",
                         collection_id: Optional[int] = None,
                         parameters: Optional[List[Dict]] = None) -> Optional[Dict]:
        """Create a new dashboard with optional parameters/filters"""
        print(f"Creating dashboard: {name}...")

        dashboard_data = {
            "name": name,
            "description": description,
            "parameters": parameters or []
        }
        if collection_id:
            dashboard_data["collection_id"] = collection_id

        result = self._request("POST", "dashboard", dashboard_data)
        if result.get("id"):
            print(f"Dashboard created! ID: {result['id']}")
            return result
        print(f"Failed to create dashboard: {result}")
        return None

    def update_dashboard(self, dashboard_id: int, updates: Dict) -> Optional[Dict]:
        """Update dashboard properties"""
        result = self._request("PUT", f"dashboard/{dashboard_id}", updates)
        if result.get("id"):
            return result
        print(f"Failed to update dashboard: {result}")
        return None

    def add_cards_to_dashboard(self, dashboard_id: int, dashcards: List[Dict]) -> Optional[Dict]:
        """Add multiple cards to a dashboard at once"""
        print(f"Adding {len(dashcards)} cards to dashboard {dashboard_id}...")

        dashboard = self._request("GET", f"dashboard/{dashboard_id}")
        if not dashboard.get("id"):
            print(f"Failed to get dashboard: {dashboard}")
            return None

        existing_dashcards = dashboard.get("dashcards", [])

        new_id = -1
        prepared_dashcards = []
        for dc in dashcards:
            dashcard_entry = {
                "id": new_id,
                "card_id": dc.get("card_id"),
                "row": dc.get("row", 0),
                "col": dc.get("col", 0),
                "size_x": dc.get("size_x", 6),
                "size_y": dc.get("size_y", 4),
                "parameter_mappings": dc.get("parameter_mappings", []),
                "visualization_settings": dc.get("visualization_settings", {})
            }
            if dc.get("dashboard_tab_id"):
                dashcard_entry["dashboard_tab_id"] = dc.get("dashboard_tab_id")
            prepared_dashcards.append(dashcard_entry)
            new_id -= 1

        updated_dashcards = existing_dashcards + prepared_dashcards
        result = self._request("PUT", f"dashboard/{dashboard_id}", {
            "dashcards": updated_dashcards
        })

        if result.get("id"):
            print(f"Cards added to dashboard!")
            return result
        print(f"Failed to add cards: {result}")
        return result

    def get_dashboards(self) -> List[Dict]:
        """Get list of dashboards"""
        return self._request("GET", "dashboard")

    def get_dashboard(self, dashboard_id: int) -> Dict:
        """Get a specific dashboard with all details"""
        return self._request("GET", f"dashboard/{dashboard_id}")

    def delete_dashboard(self, dashboard_id: int) -> bool:
        """Delete a dashboard"""
        result = self._request("DELETE", f"dashboard/{dashboard_id}")
        return result.get("status_code") in [200, 204]

    # ==================== Query Execution ====================

    def run_query(self, database_id: int, query: str) -> Dict:
        """Run a native SQL query"""
        return self._request("POST", "dataset", {
            "database": database_id,
            "type": "native",
            "native": {"query": query}
        })


def save_env_file(api_key: str, env_path: str = ".env"):
    """Save API key to .env file"""
    env_file = Path(env_path)
    content = f"METABASE_API_KEY={api_key}\n"

    if env_file.exists():
        existing = env_file.read_text()
        if "METABASE_API_KEY=" in existing:
            lines = existing.split("\n")
            lines = [l for l in lines if not l.startswith("METABASE_API_KEY=")]
            lines.insert(0, f"METABASE_API_KEY={api_key}")
            content = "\n".join(lines)
        else:
            content = f"METABASE_API_KEY={api_key}\n{existing}"

    env_file.write_text(content)
    print(f"API key saved to {env_path}")


def load_env_file(env_path: str = ".env") -> Dict[str, str]:
    """Load environment variables from .env file"""
    env_file = Path(env_path)
    env_vars = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars


def create_field_filter_tag(name: str, display_name: str, field_id: int,
                            base_type: str = "type/Text",
                            widget_type: str = "category",
                            table_alias: str = None) -> Dict:
    """
    Create a field filter template tag.
    Field filters use type="dimension" and let Metabase generate the SQL.

    Args:
        name: The variable name used in the query (e.g., "status")
        display_name: The display name shown in the UI (e.g., "Order Status")
        field_id: The Metabase field ID
        base_type: The field's base type (default: "type/Text")
        widget_type: The filter widget type (default: "category")
        table_alias: Optional table alias for JOINs (e.g., "o.status" for "orders AS o")
    """
    tag = {
        "id": name,
        "name": name,
        "display-name": display_name,
        "type": "dimension",
        "dimension": ["field", field_id, None],
        "widget-type": widget_type
    }
    # Add alias if specified (for queries with table aliases/JOINs)
    if table_alias:
        tag["alias"] = table_alias
    return {name: tag}


def create_date_filter_tag(name: str, display_name: str, field_id: int,
                           table_alias: str = None) -> Dict:
    """
    Create a date field filter template tag.

    Args:
        name: The variable name used in the query
        display_name: The display name shown in the UI
        field_id: The Metabase field ID
        table_alias: Optional table alias for JOINs (e.g., "dm.metric_date")
    """
    tag = {
        "id": name,
        "name": name,
        "display-name": display_name,
        "type": "dimension",
        "dimension": ["field", field_id, None],
        "widget-type": "date/all-options"
    }
    if table_alias:
        tag["alias"] = table_alias
    return {name: tag}


def create_comprehensive_dashboard(client: MetabaseClient, gizmo_db_id: int) -> Optional[int]:
    """
    Create a comprehensive test dashboard with field filters and multiple chart types.
    Uses GizmoSQL data from sales, hr, and analytics schemas.
    Returns the dashboard ID.
    """
    print("\n" + "=" * 60)
    print("Creating Comprehensive Test Dashboard with Field Filters")
    print("=" * 60)

    # Sync and cache field metadata
    client.sync_database(gizmo_db_id)
    time.sleep(3)
    client.cache_database_fields(gizmo_db_id)

    # Get field info for filter mappings
    status_field = client.get_field_info(gizmo_db_id, "sales", "orders", "status")
    country_field = client.get_field_info(gizmo_db_id, "sales", "customers", "country")
    order_date_field = client.get_field_info(gizmo_db_id, "sales", "orders", "order_date")
    dept_name_field = client.get_field_info(gizmo_db_id, "hr", "departments", "department_name")
    channel_field = client.get_field_info(gizmo_db_id, "analytics", "campaign_performance", "channel")
    metric_date_field = client.get_field_info(gizmo_db_id, "analytics", "daily_metrics", "metric_date")

    print(f"Field IDs:")
    print(f"  status: {status_field.get('id') if status_field else None}")
    print(f"  country: {country_field.get('id') if country_field else None}")
    print(f"  order_date: {order_date_field.get('id') if order_date_field else None}")
    print(f"  department_name: {dept_name_field.get('id') if dept_name_field else None}")
    print(f"  channel: {channel_field.get('id') if channel_field else None}")
    print(f"  metric_date: {metric_date_field.get('id') if metric_date_field else None}")

    # Build template tags for field filters
    # Non-aliased tags for simple single-table queries
    status_tag = create_field_filter_tag("status", "Order Status", status_field["id"]) if status_field else {}
    country_tag = create_field_filter_tag("country", "Country", country_field["id"]) if country_field else {}
    dept_tag = create_field_filter_tag("department", "Department", dept_name_field["id"]) if dept_name_field else {}
    channel_tag = create_field_filter_tag("channel", "Marketing Channel", channel_field["id"]) if channel_field else {}
    date_tag = create_date_filter_tag("date_range", "Date Range", metric_date_field["id"]) if metric_date_field else {}
    order_date_tag = create_date_filter_tag("order_date", "Order Date", order_date_field["id"]) if order_date_field else {}

    # Aliased tags for JOIN queries - must match the table aliases used in SQL
    # For "orders o" use "o.status", for "customers c" use "c.country", etc.
    status_tag_o = create_field_filter_tag("status", "Order Status", status_field["id"], table_alias="o.status") if status_field else {}
    country_tag_c = create_field_filter_tag("country", "Country", country_field["id"], table_alias="c.country") if country_field else {}
    dept_tag_d = create_field_filter_tag("department", "Department", dept_name_field["id"], table_alias="d.department_name") if dept_name_field else {}

    created_cards = []

    # ==================== SALES CARDS ====================

    # Card 1: Total Revenue - with status field filter
    # Field filter syntax: WHERE {{filter}} - Metabase generates the column comparison
    card = client.create_native_card(
        name="Total Revenue",
        database_id=gizmo_db_id,
        query="SELECT SUM(total_amount) AS total_revenue FROM sales.orders [[WHERE {{status}}]]",
        display="scalar",
        visualization_settings={
            "scalar.field": "total_revenue",
            "scalar.prefix": "$"
        },
        template_tags=status_tag if status_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 0, "col": 0, "size_x": 6, "size_y": 3,
            "filters": ["status"] if status_field else []
        })

    # Card 2: Order Count
    card = client.create_native_card(
        name="Total Orders",
        database_id=gizmo_db_id,
        query="SELECT COUNT(*) AS order_count FROM sales.orders [[WHERE {{status}}]]",
        display="scalar",
        visualization_settings={},
        template_tags=status_tag if status_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 0, "col": 6, "size_x": 6, "size_y": 3,
            "filters": ["status"] if status_field else []
        })

    # Card 3: Average Order Value
    card = client.create_native_card(
        name="Average Order Value",
        database_id=gizmo_db_id,
        query="SELECT AVG(total_amount) AS avg_order FROM sales.orders [[WHERE {{status}}]]",
        display="scalar",
        visualization_settings={
            "scalar.prefix": "$",
            "scalar.decimals": 2
        },
        template_tags=status_tag if status_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 0, "col": 12, "size_x": 6, "size_y": 3,
            "filters": ["status"] if status_field else []
        })

    # Card 4: Customer Count with country field filter
    card = client.create_native_card(
        name="Total Customers",
        database_id=gizmo_db_id,
        query="SELECT COUNT(*) AS customer_count FROM sales.customers [[WHERE {{country}}]]",
        display="scalar",
        visualization_settings={},
        template_tags=country_tag if country_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 0, "col": 18, "size_x": 6, "size_y": 3,
            "filters": ["country"] if country_field else []
        })

    # Card 5: Revenue by Country (Bar Chart)
    # For JOINs with multiple field filters, use aliased tags matching table aliases
    combined_tags_oc = {**country_tag_c, **status_tag_o}
    card = client.create_native_card(
        name="Revenue by Country",
        database_id=gizmo_db_id,
        query="""
            SELECT c.country, SUM(o.total_amount) AS revenue
            FROM sales.orders o
            JOIN sales.customers c ON o.customer_id = c.customer_id
            WHERE 1=1 [[AND {{country}}]] [[AND {{status}}]]
            GROUP BY c.country
            ORDER BY revenue DESC
        """,
        display="bar",
        visualization_settings={
            "graph.dimensions": ["country"],
            "graph.metrics": ["revenue"],
            "graph.x_axis.title_text": "Country",
            "graph.y_axis.title_text": "Revenue ($)"
        },
        template_tags=combined_tags_oc if combined_tags_oc else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 3, "col": 0, "size_x": 12, "size_y": 6,
            "filters": ["country", "status"]
        })

    # Card 6: Orders by Status (Pie Chart) - no filter, shows all statuses
    card = client.create_native_card(
        name="Orders by Status",
        database_id=gizmo_db_id,
        query="""
            SELECT status, COUNT(*) AS order_count
            FROM sales.orders
            GROUP BY status
        """,
        display="pie",
        visualization_settings={
            "pie.dimension": "status",
            "pie.metric": "order_count",
            "pie.show_legend": True,
            "pie.show_labels": True,
            "pie.percent_visibility": "inside"
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 3, "col": 12, "size_x": 6, "size_y": 6,
            "filters": []
        })

    # Card 7: Top Products by Revenue (Horizontal Bar)
    # Uses alias "o" for orders table - need aliased status tag
    card = client.create_native_card(
        name="Top Products by Revenue",
        database_id=gizmo_db_id,
        query="""
            SELECT p.product_name, SUM(oi.quantity * oi.unit_price) AS revenue
            FROM sales.order_items oi
            JOIN sales.products p ON oi.product_id = p.product_id
            JOIN sales.orders o ON oi.order_id = o.order_id
            WHERE 1=1 [[AND {{status}}]]
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 10
        """,
        display="row",
        visualization_settings={
            "graph.dimensions": ["product_name"],
            "graph.metrics": ["revenue"]
        },
        template_tags=status_tag_o if status_tag_o else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 3, "col": 18, "size_x": 6, "size_y": 6,
            "filters": ["status"] if status_field else []
        })

    # Card 8: Product Categories (Pie Chart)
    card = client.create_native_card(
        name="Products by Category",
        database_id=gizmo_db_id,
        query="""
            SELECT category, COUNT(*) AS product_count
            FROM sales.products
            GROUP BY category
        """,
        display="pie",
        visualization_settings={
            "pie.dimension": "category",
            "pie.metric": "product_count",
            "pie.show_legend": True,
            "pie.percent_visibility": "inside"
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 9, "col": 0, "size_x": 8, "size_y": 5,
            "filters": []
        })

    # Card 9: Recent Orders Table with multiple field filters
    # Uses aliases "o" for orders and "c" for customers - need aliased tags
    all_sales_tags_oc = {**status_tag_o, **country_tag_c}
    card = client.create_native_card(
        name="Recent Orders",
        database_id=gizmo_db_id,
        query="""
            SELECT o.order_id, c.first_name || ' ' || c.last_name AS customer,
                   c.country, o.order_date, o.total_amount, o.status
            FROM sales.orders o
            JOIN sales.customers c ON o.customer_id = c.customer_id
            WHERE 1=1 [[AND {{status}}]] [[AND {{country}}]]
            ORDER BY o.order_date DESC
        """,
        display="table",
        visualization_settings={
            "table.pivot": False,
            "column_settings": {
                '["name","total_amount"]': {"prefix": "$"}
            }
        },
        template_tags=all_sales_tags_oc if all_sales_tags_oc else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 9, "col": 8, "size_x": 16, "size_y": 5,
            "filters": ["status", "country"]
        })

    # ==================== HR CARDS ====================

    # Card 10: Department Budget (Bar Chart)
    card = client.create_native_card(
        name="Department Budgets",
        database_id=gizmo_db_id,
        query="""
            SELECT department_name, budget
            FROM hr.departments
            [[WHERE {{department}}]]
            ORDER BY budget DESC
        """,
        display="bar",
        visualization_settings={
            "graph.dimensions": ["department_name"],
            "graph.metrics": ["budget"],
            "graph.colors": ["#509EE3"]
        },
        template_tags=dept_tag if dept_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 14, "col": 0, "size_x": 12, "size_y": 6,
            "filters": ["department"] if dept_name_field else []
        })

    # Card 11: Employee Count
    # Uses alias "d" for departments - need aliased dept tag
    card = client.create_native_card(
        name="Total Employees",
        database_id=gizmo_db_id,
        query="""
            SELECT COUNT(*) AS employee_count FROM hr.employees e
            JOIN hr.departments d ON e.department_id = d.department_id
            WHERE 1=1 [[AND {{department}}]]
        """,
        display="scalar",
        visualization_settings={},
        template_tags=dept_tag_d if dept_tag_d else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 14, "col": 12, "size_x": 6, "size_y": 3,
            "filters": ["department"] if dept_name_field else []
        })

    # Card 12: Average Salary (Gauge)
    # Uses alias "d" for departments - need aliased dept tag
    card = client.create_native_card(
        name="Average Salary",
        database_id=gizmo_db_id,
        query="""
            SELECT AVG(e.salary) AS avg_salary FROM hr.employees e
            JOIN hr.departments d ON e.department_id = d.department_id
            WHERE 1=1 [[AND {{department}}]]
        """,
        display="gauge",
        visualization_settings={
            "gauge.segments": [
                {"min": 0, "max": 80000, "color": "#84BB4C", "label": "Entry"},
                {"min": 80000, "max": 120000, "color": "#F9CF48", "label": "Mid"},
                {"min": 120000, "max": 200000, "color": "#ED6E6E", "label": "Senior"}
            ]
        },
        template_tags=dept_tag_d if dept_tag_d else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 14, "col": 18, "size_x": 6, "size_y": 3,
            "filters": ["department"] if dept_name_field else []
        })

    # Card 13: Salary by Department (Stacked Bar)
    # Uses alias "d" for departments - need aliased dept tag
    card = client.create_native_card(
        name="Salary by Department",
        database_id=gizmo_db_id,
        query="""
            SELECT d.department_name, e.job_title, SUM(e.salary) AS total_salary
            FROM hr.employees e
            JOIN hr.departments d ON e.department_id = d.department_id
            WHERE 1=1 [[AND {{department}}]]
            GROUP BY d.department_name, e.job_title
            ORDER BY d.department_name
        """,
        display="bar",
        visualization_settings={
            "graph.dimensions": ["department_name", "job_title"],
            "graph.metrics": ["total_salary"],
            "stackable.stack_type": "stacked"
        },
        template_tags=dept_tag_d if dept_tag_d else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 17, "col": 12, "size_x": 12, "size_y": 6,
            "filters": ["department"] if dept_name_field else []
        })

    # Card 14: Time Off by Type (Pie)
    card = client.create_native_card(
        name="Time Off by Type",
        database_id=gizmo_db_id,
        query="""
            SELECT request_type, COUNT(*) AS request_count
            FROM hr.time_off_requests
            GROUP BY request_type
        """,
        display="pie",
        visualization_settings={
            "pie.dimension": "request_type",
            "pie.metric": "request_count",
            "pie.show_legend": True
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 20, "col": 0, "size_x": 6, "size_y": 5,
            "filters": []
        })

    # Card 15: Time Off Status (Funnel)
    card = client.create_native_card(
        name="Request Status Funnel",
        database_id=gizmo_db_id,
        query="""
            SELECT status, COUNT(*) AS count
            FROM hr.time_off_requests
            GROUP BY status
            ORDER BY count DESC
        """,
        display="funnel",
        visualization_settings={
            "funnel.dimension": "status",
            "funnel.metric": "count"
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 20, "col": 6, "size_x": 6, "size_y": 5,
            "filters": []
        })

    # Card 16: Employee Directory Table
    # Uses alias "d" for departments - need aliased dept tag
    card = client.create_native_card(
        name="Employee Directory",
        database_id=gizmo_db_id,
        query="""
            SELECT e.first_name || ' ' || e.last_name AS employee_name,
                   e.email, d.department_name, e.job_title, e.hire_date, e.salary
            FROM hr.employees e
            JOIN hr.departments d ON e.department_id = d.department_id
            WHERE 1=1 [[AND {{department}}]]
            ORDER BY e.hire_date DESC
        """,
        display="table",
        visualization_settings={"table.pivot": False},
        template_tags=dept_tag_d if dept_tag_d else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 25, "col": 0, "size_x": 24, "size_y": 6,
            "filters": ["department"] if dept_name_field else []
        })

    # ==================== ANALYTICS CARDS ====================

    # Card 17: Daily Page Views (Line Chart) with date field filter
    card = client.create_native_card(
        name="Daily Page Views Trend",
        database_id=gizmo_db_id,
        query="""
            SELECT metric_date, page_views
            FROM analytics.daily_metrics
            [[WHERE {{date_range}}]]
            ORDER BY metric_date
        """,
        display="line",
        visualization_settings={
            "graph.dimensions": ["metric_date"],
            "graph.metrics": ["page_views"],
            "graph.x_axis.scale": "timeseries"
        },
        template_tags=date_tag if date_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 31, "col": 0, "size_x": 12, "size_y": 6,
            "filters": ["date_range"] if metric_date_field else []
        })

    # Card 18: Revenue vs Visitors (Line)
    card = client.create_native_card(
        name="Revenue vs Visitors",
        database_id=gizmo_db_id,
        query="""
            SELECT metric_date, revenue, unique_visitors
            FROM analytics.daily_metrics
            [[WHERE {{date_range}}]]
            ORDER BY metric_date
        """,
        display="line",
        visualization_settings={
            "graph.dimensions": ["metric_date"],
            "graph.metrics": ["revenue", "unique_visitors"],
            "graph.x_axis.scale": "timeseries",
            "series_settings": {
                "revenue": {"axis": "left"},
                "unique_visitors": {"axis": "right"}
            }
        },
        template_tags=date_tag if date_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 31, "col": 12, "size_x": 12, "size_y": 6,
            "filters": ["date_range"] if metric_date_field else []
        })

    # Card 19: Bounce Rate (Area)
    card = client.create_native_card(
        name="Bounce Rate Trend",
        database_id=gizmo_db_id,
        query="""
            SELECT metric_date, bounce_rate
            FROM analytics.daily_metrics
            [[WHERE {{date_range}}]]
            ORDER BY metric_date
        """,
        display="area",
        visualization_settings={
            "graph.dimensions": ["metric_date"],
            "graph.metrics": ["bounce_rate"],
            "graph.x_axis.scale": "timeseries"
        },
        template_tags=date_tag if date_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 37, "col": 0, "size_x": 8, "size_y": 5,
            "filters": ["date_range"] if metric_date_field else []
        })

    # Card 20: Conversion Rate Progress
    card = client.create_native_card(
        name="Latest Conversion Rate",
        database_id=gizmo_db_id,
        query="""
            SELECT conversion_rate
            FROM analytics.daily_metrics
            ORDER BY metric_date DESC
            LIMIT 1
        """,
        display="progress",
        visualization_settings={
            "progress.goal": 5.0,
            "progress.color": "#84BB4C"
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 37, "col": 8, "size_x": 8, "size_y": 5,
            "filters": []
        })

    # Card 21: Events by Type (Bar)
    card = client.create_native_card(
        name="Events by Type",
        database_id=gizmo_db_id,
        query="""
            SELECT event_type, COUNT(*) AS event_count
            FROM analytics.website_events
            GROUP BY event_type
            ORDER BY event_count DESC
        """,
        display="bar",
        visualization_settings={
            "graph.dimensions": ["event_type"],
            "graph.metrics": ["event_count"]
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 37, "col": 16, "size_x": 8, "size_y": 5,
            "filters": []
        })

    # Card 22: Device Type Distribution (Pie)
    card = client.create_native_card(
        name="Traffic by Device",
        database_id=gizmo_db_id,
        query="""
            SELECT device_type, COUNT(*) AS sessions
            FROM analytics.website_events
            GROUP BY device_type
        """,
        display="pie",
        visualization_settings={
            "pie.dimension": "device_type",
            "pie.metric": "sessions",
            "pie.show_legend": True
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 42, "col": 0, "size_x": 8, "size_y": 6,
            "filters": []
        })

    # Card 23: Browser Distribution (Pie)
    card = client.create_native_card(
        name="Traffic by Browser",
        database_id=gizmo_db_id,
        query="""
            SELECT browser, COUNT(*) AS sessions
            FROM analytics.website_events
            GROUP BY browser
        """,
        display="pie",
        visualization_settings={
            "pie.dimension": "browser",
            "pie.metric": "sessions",
            "pie.show_legend": True
        }
    )
    if card:
        created_cards.append({
            "card": card, "row": 42, "col": 8, "size_x": 8, "size_y": 6,
            "filters": []
        })

    # Card 24: Metrics Summary Table
    card = client.create_native_card(
        name="Daily Metrics Summary",
        database_id=gizmo_db_id,
        query="""
            SELECT metric_date, total_visitors, unique_visitors, page_views,
                   avg_session_duration_seconds, bounce_rate, conversion_rate, revenue
            FROM analytics.daily_metrics
            [[WHERE {{date_range}}]]
            ORDER BY metric_date DESC
        """,
        display="table",
        visualization_settings={"table.pivot": False},
        template_tags=date_tag if date_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 42, "col": 16, "size_x": 8, "size_y": 6,
            "filters": ["date_range"] if metric_date_field else []
        })

    # ==================== CAMPAIGN CARDS ====================

    # Card 25: Campaign ROI (Scatter)
    card = client.create_native_card(
        name="Campaign ROI Analysis",
        database_id=gizmo_db_id,
        query="""
            SELECT campaign_name, spend, revenue,
                   (revenue - spend) AS profit,
                   CASE WHEN spend > 0 THEN (revenue / spend) ELSE 0 END AS roi
            FROM analytics.campaign_performance
            [[WHERE {{channel}}]]
        """,
        display="scatter",
        visualization_settings={
            "scatter.bubble": "profit",
            "graph.dimensions": ["campaign_name"],
            "graph.metrics": ["spend", "revenue"]
        },
        template_tags=channel_tag if channel_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 48, "col": 0, "size_x": 12, "size_y": 8,
            "filters": ["channel"] if channel_field else []
        })

    # Card 26: Spend vs Revenue by Channel (Bar)
    card = client.create_native_card(
        name="Spend vs Revenue by Channel",
        database_id=gizmo_db_id,
        query="""
            SELECT channel, SUM(spend) AS total_spend, SUM(revenue) AS total_revenue
            FROM analytics.campaign_performance
            [[WHERE {{channel}}]]
            GROUP BY channel
        """,
        display="bar",
        visualization_settings={
            "graph.dimensions": ["channel"],
            "graph.metrics": ["total_spend", "total_revenue"]
        },
        template_tags=channel_tag if channel_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 48, "col": 12, "size_x": 12, "size_y": 8,
            "filters": ["channel"] if channel_field else []
        })

    # Card 27: Campaign Conversion Metrics (Table)
    card = client.create_native_card(
        name="Campaign Conversion Metrics",
        database_id=gizmo_db_id,
        query="""
            SELECT campaign_name, channel, impressions, clicks, conversions,
                   CASE WHEN impressions > 0 THEN CAST(clicks AS FLOAT) / impressions * 100 ELSE 0 END AS ctr,
                   CASE WHEN clicks > 0 THEN CAST(conversions AS FLOAT) / clicks * 100 ELSE 0 END AS conversion_rate
            FROM analytics.campaign_performance
            [[WHERE {{channel}}]]
            ORDER BY conversions DESC
        """,
        display="table",
        visualization_settings={"table.pivot": False},
        template_tags=channel_tag if channel_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 56, "col": 0, "size_x": 16, "size_y": 6,
            "filters": ["channel"] if channel_field else []
        })

    # Card 28: Budget Utilization (Row)
    card = client.create_native_card(
        name="Budget Utilization",
        database_id=gizmo_db_id,
        query="""
            SELECT campaign_name, budget, spend,
                   CASE WHEN budget > 0 THEN (spend / budget) * 100 ELSE 0 END AS utilization_pct
            FROM analytics.campaign_performance
            [[WHERE {{channel}}]]
            ORDER BY utilization_pct DESC
        """,
        display="row",
        visualization_settings={
            "graph.dimensions": ["campaign_name"],
            "graph.metrics": ["utilization_pct"]
        },
        template_tags=channel_tag if channel_tag else None
    )
    if card:
        created_cards.append({
            "card": card, "row": 56, "col": 16, "size_x": 8, "size_y": 6,
            "filters": ["channel"] if channel_field else []
        })

    # Cards 29-32: KPI Scalars
    for kpi_config in [
        {"name": "Total Ad Spend", "query": "SELECT SUM(spend) AS value FROM analytics.campaign_performance [[WHERE {{channel}}]]", "prefix": "$", "col": 0},
        {"name": "Total Conversions", "query": "SELECT SUM(conversions) AS value FROM analytics.campaign_performance [[WHERE {{channel}}]]", "prefix": "", "col": 6},
        {"name": "Campaign Revenue", "query": "SELECT SUM(revenue) AS value FROM analytics.campaign_performance [[WHERE {{channel}}]]", "prefix": "$", "col": 12},
        {"name": "Overall ROI", "query": "SELECT CASE WHEN SUM(spend) > 0 THEN (SUM(revenue) - SUM(spend)) / SUM(spend) * 100 ELSE 0 END AS value FROM analytics.campaign_performance [[WHERE {{channel}}]]", "prefix": "", "suffix": "%", "col": 18},
    ]:
        viz = {}
        if kpi_config.get("prefix"):
            viz["scalar.prefix"] = kpi_config["prefix"]
        if kpi_config.get("suffix"):
            viz["scalar.suffix"] = kpi_config["suffix"]
            viz["scalar.decimals"] = 1
        card = client.create_native_card(
            name=kpi_config["name"],
            database_id=gizmo_db_id,
            query=kpi_config["query"],
            display="scalar",
            visualization_settings=viz,
            template_tags=channel_tag if channel_tag else None
        )
        if card:
            created_cards.append({
                "card": card, "row": 62, "col": kpi_config["col"], "size_x": 6, "size_y": 3,
                "filters": ["channel"] if channel_field else []
            })

    # ==================== Create Dashboard ====================

    # Dashboard parameters with field filter types
    dashboard_params = []

    if status_field:
        dashboard_params.append({
            "id": "status",
            "name": "Order Status",
            "slug": "status",
            "type": "string/=",
            "sectionId": "string"
        })

    if country_field:
        dashboard_params.append({
            "id": "country",
            "name": "Country",
            "slug": "country",
            "type": "string/=",
            "sectionId": "string"
        })

    if dept_name_field:
        dashboard_params.append({
            "id": "department",
            "name": "Department",
            "slug": "department",
            "type": "string/=",
            "sectionId": "string"
        })

    if channel_field:
        dashboard_params.append({
            "id": "channel",
            "name": "Marketing Channel",
            "slug": "channel",
            "type": "string/=",
            "sectionId": "string"
        })

    if metric_date_field:
        dashboard_params.append({
            "id": "date_range",
            "name": "Date Range",
            "slug": "date_range",
            "type": "date/all-options",
            "sectionId": "date"
        })

    dashboard = client.create_dashboard(
        name="FlightSQL Driver Test Dashboard v2",
        description="Comprehensive test dashboard with field filters and multiple chart types for Arrow Flight SQL driver testing",
        parameters=dashboard_params
    )

    if not dashboard:
        print("Failed to create dashboard!")
        return None

    dashboard_id = dashboard["id"]

    # Build dashcards with parameter mappings
    dashcards = []
    for item in created_cards:
        card = item["card"]
        card_viz_settings = card.get("visualization_settings", {})

        # Build parameter mappings for field filters
        param_mappings = []
        for filter_name in item.get("filters", []):
            param_mappings.append({
                "parameter_id": filter_name,
                "card_id": card["id"],
                "target": ["dimension", ["template-tag", filter_name]]
            })

        dashcard = {
            "card_id": card["id"],
            "row": item.get("row", 0),
            "col": item.get("col", 0),
            "size_x": item.get("size_x", 8),
            "size_y": item.get("size_y", 6),
            "visualization_settings": card_viz_settings,
            "parameter_mappings": param_mappings
        }
        dashcards.append(dashcard)

    # Add all cards to dashboard
    client.add_cards_to_dashboard(dashboard_id, dashcards)

    print(f"\nDashboard created successfully!")
    print(f"Dashboard URL: {client.config.base_url}/dashboard/{dashboard_id}")
    print(f"Total cards created: {len(created_cards)}")
    print(f"Filters configured: {[p['name'] for p in dashboard_params]}")

    return dashboard_id


def main():
    """Main setup and test function"""
    print("=" * 60)
    print("Metabase Automated Setup & Testing")
    print("=" * 60)

    # Load .env file first
    env_vars = load_env_file()
    for key, value in env_vars.items():
        if key not in os.environ:
            os.environ[key] = value

    config = MetabaseConfig()
    client = MetabaseClient(config)

    # Step 1: Wait for Metabase
    if not client.wait_for_ready():
        sys.exit(1)

    # Step 2: Check setup status and perform setup or login
    props = client.get_session_properties()
    has_user = props.get("has-user-setup", False)

    # Try to use existing API key from env first
    env_key = os.environ.get("METABASE_API_KEY")
    if env_key:
        print("\n--- Using API key from .env ---")
        client.set_api_key(env_key)
        dbs = client.get_databases()
        if isinstance(dbs, dict) and "API" not in str(dbs.get("text", "")):
            print("API key is valid!")
        elif isinstance(dbs, list) or (isinstance(dbs, dict) and "data" in dbs):
            print("API key is valid!")
        else:
            print(f"API key invalid, will try login...")
            client.api_key = None

    if not client.api_key:
        if not has_user:
            print("\n--- Performing Initial Setup ---")
            if not client.setup():
                print("Setup failed!")
                sys.exit(1)
        else:
            print("\n--- Metabase already configured, logging in ---")
            if not client.login():
                print("Login failed!")
                sys.exit(1)

    # Step 3: Create or verify API key
    print("\n--- API Key Management ---")
    api_keys = client.get_api_keys()
    if isinstance(api_keys, list) and len(api_keys) > 0:
        print(f"Found {len(api_keys)} existing API key(s)")
    else:
        new_key = client.create_api_key("automation-key")
        if new_key:
            save_env_file(new_key)
            client.set_api_key(new_key)

    # Step 4: Create database connections
    print("\n--- Database Connections ---")
    gizmo = client.create_gizmosql_connection()
    spice = client.create_spice_connection()

    # Step 5: Sync databases
    print("\n--- Syncing Databases ---")
    if gizmo:
        client.sync_database(gizmo["id"])
        time.sleep(5)
        meta = client.get_database_metadata(gizmo["id"])
        tables = meta.get("tables", [])
        print(f"GizmoSQL: {len(tables)} tables synced")

    if spice:
        client.sync_database(spice["id"])
        time.sleep(5)
        meta = client.get_database_metadata(spice["id"])
        tables = meta.get("tables", [])
        print(f"Spice: {len(tables)} tables synced")

    # Step 6: Test queries
    print("\n--- Testing Queries ---")
    if gizmo:
        result = client.run_query(gizmo["id"], "SELECT COUNT(*) as cnt FROM hr.departments")
        if result.get("status") == "completed":
            rows = result.get("data", {}).get("rows", [])
            print(f"GizmoSQL query OK: {rows}")
        else:
            print(f"GizmoSQL query failed: {result.get('error', 'Unknown error')}")

    # Step 7: Create comprehensive test dashboard
    if gizmo:
        dashboard_id = create_comprehensive_dashboard(client, gizmo["id"])
        if dashboard_id:
            print(f"\nComprehensive Dashboard URL: {config.base_url}/dashboard/{dashboard_id}")

    print("\n" + "=" * 60)
    print("Setup Complete!")
    print("=" * 60)
    print(f"Metabase URL: {config.base_url}")
    print(f"Admin Email: {config.admin_email}")
    print(f"Admin Password: {config.admin_password}")
    print("=" * 60)


if __name__ == "__main__":
    main()
