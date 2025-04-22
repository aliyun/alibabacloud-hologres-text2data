from typing import Any
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus # For URL encoding

# Define version constant
APP_VERSION = "0.1.0"

def get_db_schema(
        db_type: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        table_names: str | None = None
) -> dict[str, Any] | None:
    """
    Get database table structure information
    :param db_type: Database type (mysql/oracle/sqlserver/hologres)
    :param host: Host address
    :param port: Port number
    :param database: Database name
    :param username: Username
    :param password: Password
    :param table_names: Tables to query, comma-separated string, if None, query all tables
    :return: Dictionary containing all table structure information
    """
    result: dict[str, Any] = {}
    # Build connection URL
    driver = {
        'mysql': 'pymysql',
        'oracle': 'cx_oracle',
        'sqlserver': 'pymssql',
        'hologres': 'psycopg2'
    }.get(db_type.lower(), '')

    encoded_username = quote_plus(username)
    encoded_password = quote_plus(password)
    
    # Handle Hologres type, use PostgreSQL connection method
    actual_db_type = db_type.lower()
    if actual_db_type == 'hologres':
        actual_db_type = 'postgresql'
    
    # Create database engine
    connection_url = f'{actual_db_type}+{driver}://{encoded_username}:{encoded_password}@{host}:{port}/{database}'
    
    # Add application_name parameter with version for PostgreSQL/Hologres
    if db_type.lower() == 'hologres' or db_type.lower() == 'postgresql':
        connection_url += f'?application_name=hologres_text2data_from_dify_v{APP_VERSION}'
    
    engine = create_engine(connection_url)
    inspector = inspect(engine)

    # SQL statements for getting column comments
    column_comment_sql = {
        'mysql': f"SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = :table_name AND COLUMN_NAME = :column_name",
        'oracle': "SELECT COMMENTS FROM ALL_COL_COMMENTS WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name",
        'sqlserver': "SELECT CAST(ep.value AS NVARCHAR(MAX)) FROM sys.columns c LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id WHERE OBJECT_NAME(c.object_id) = :table_name AND c.name = :column_name",
        'hologres': """
                SELECT
                    d.description AS column_comment
                FROM
                    pg_class c
                    LEFT JOIN pg_namespace n ON c.relnamespace = n.oid
                    LEFT JOIN pg_attribute a ON c.oid = a.attrelid
                    LEFT JOIN pg_description d ON a.attrelid = d.objoid AND a.attnum = d.objsubid
                WHERE
                    c.relkind IN ('r', 'f', 'v')
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                    AND n.nspname = :schema_name
                    AND c.relname = :table_name 
                    AND a.attname = :column_name
                    AND n.nspname = :schema_name;
        """
    }.get(db_type.lower(), "")

    try:
        # Get all table names
        all_tables = []
        
        # For PostgreSQL/Hologres, consider schema structure
        if db_type.lower() == 'hologres' or db_type.lower() == 'postgresql':
            # Get all schemas
            schemas = inspector.get_schema_names()
            # Exclude system schemas
            excluded_schemas = ['pg_catalog', 'information_schema']
            # For Hologres, exclude additional system schemas
            if db_type.lower() == 'hologres':
                excluded_schemas.extend(['hologres', 'hologres_statistic', 'hologres_streaming_mv'])
            
            schemas = [s for s in schemas if s not in excluded_schemas and not s.startswith('pg_')]
            
            # Get tables for each schema
            for schema in schemas:
                # Get regular tables
                schema_tables = inspector.get_table_names(schema=schema)
                all_tables.extend([f"{schema}.{table}" for table in schema_tables])
                
                # Get views
                schema_views = inspector.get_view_names(schema=schema)
                all_tables.extend([f"{schema}.{view}" for view in schema_views])
                
                # For Hologres, get partition tables and foreign tables with additional SQL query
                if db_type.lower() == 'hologres' or db_type.lower() == 'postgresql':
                    try:
                        with engine.connect() as conn:
                            # Query for partition parent tables and foreign tables
                            special_tables_sql = text("""
                                SELECT c.relname as table_name
                                FROM pg_class c
                                JOIN pg_namespace n ON c.relnamespace = n.oid
                                WHERE n.nspname = :schema_name
                                AND c.relkind IN ('p', 'f')  -- 'p' for partition parent tables, 'f' for foreign tables
                                AND NOT EXISTS (
                                    SELECT 1 FROM pg_inherits i
                                    WHERE i.inhrelid = c.oid
                                )
                            """)
                            # Modified: Use different variable name query_result instead of result
                            query_result = conn.execute(special_tables_sql, {"schema_name": schema})
                            # Fix: Access the first element of the tuple using index 0, or use keys() to get column names
                            special_tables = []
                            for row in query_result:
                                if hasattr(row, '_mapping'):  # SQLAlchemy 1.4+
                                    special_tables.append(row._mapping['table_name'])
                                elif hasattr(row, 'keys'):  # Compatible with older versions
                                    special_tables.append(row[row.keys().index('table_name')])
                                else:  # Use index directly
                                    special_tables.append(row[0])
                            all_tables.extend([f"{schema}.{table}" for table in special_tables])
                    except SQLAlchemyError as e:
                        print(f"Warning: failed to get special tables for schema {schema}: {e}")
        else:
            # Other database types remain unchanged
            all_tables = inspector.get_table_names()

        # If table_names is specified, filter table names
        target_tables = all_tables

        if table_names:
            target_tables = [table.strip() for table in table_names.split(',')]
            # Filter for tables that actually exist
            target_tables = [table for table in target_tables if table in all_tables]
        print(f"Retrieving table metadata for {len(target_tables)} tables...")
        for table_name in target_tables:
            # Handle table names that may include schema
            schema_name = 'public'  # Default schema
            actual_table_name = table_name
            
            if '.' in table_name and (db_type.lower() == 'hologres' or db_type.lower() == 'postgresql'):
                schema_name, actual_table_name = table_name.split('.', 1)
            
            # Get table comments
            table_comment = ""
            try:
                table_comment = inspector.get_table_comment(actual_table_name, schema=schema_name).get("text") or ""
            except SQLAlchemyError as e:
                raise ValueError(f"Failed to retrieve table comments: {str(e)}")

            table_info = {
                'comment': table_comment,
                'columns': []
            }

            for column in inspector.get_columns(actual_table_name, schema=schema_name):
                # Get column comments
                column_comment = ""
                try:
                    with engine.connect() as conn:
                        stmt = text(column_comment_sql)
                        params = {
                            'table_name': actual_table_name,
                            'column_name': column['name']
                        }
                        # Add schema parameter for PostgreSQL/Hologres
                        if db_type.lower() == 'hologres' or db_type.lower() == 'postgresql':
                            params['schema_name'] = schema_name
                            
                        column_comment = conn.execute(stmt, params).scalar() or ""
                except SQLAlchemyError as e:
                    print(f"Warning: failed to get comment for {table_name}.{column['name']} - {e}")
                    column_comment = ""

                table_info['columns'].append({
                    'name': column['name'],
                    'comment': column_comment,
                    'type': str(column['type'])
                })

            result[table_name] = table_info
        return result
    except SQLAlchemyError as e:
        raise ValueError(f"Failed to retrieve database table metadata: {str(e)}")
    finally:
        engine.dispose()

def format_schema_dsl(schema: dict[str, Any], with_type: bool = True, with_comment: bool = False) -> str:
    """
    Compress database table structure into DSL format
    :param schema: Structure returned by get_db_schema
    :param with_type: Whether to keep field types
    :param with_comment: Whether to keep field comments
    :return: Compressed DSL string
    """
    type_aliases = {
        'INTEGER': 'i', 'INT': 'i', 'BIGINT': 'i', 'SMALLINT': 'i', 'TINYINT': 'i',
        'VARCHAR': 's', 'TEXT': 's', 'CHAR': 's',
        'DATETIME': 'dt', 'TIMESTAMP': 'dt', 'DATE': 'dt',
        'DECIMAL': 'f', 'NUMERIC': 'f', 'FLOAT': 'f', 'DOUBLE': 'f',
        'BOOLEAN': 'b', 'BOOL': 'b',
        'JSON': 'j'
    }
    lines = []
    for table_name, table_data in schema.items():
        column_parts = []

        for col in table_data['columns']:
            parts = [col['name']]
            if with_type:
                raw_type = col['type'].split('(')[0].upper()
                col_type = type_aliases.get(raw_type, raw_type.lower())
                parts.append(col_type)
            if with_comment and col.get('comment'):
                parts.append(f"# {col['comment']}")
            column_parts.append(":".join(parts))

        # Build table comment
        if with_comment and table_data.get('comment'):
            lines.append(f"# {table_data['comment']}")
        lines.append(f"T:{table_name}({', '.join(column_parts)})")

    return "\n".join(lines)

def execute_sql(
        db_type: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str,
        sql: str,
        params: dict[str, Any] | None = None
) -> list[dict[str, Any]] | dict[str, Any] | None:
    """
    Function to connect to different types of databases and execute SQL statements.
    
    Parameters:
        db_type: Database type, e.g., 'mysql', 'oracle', 'sqlserver', 'hologres'
        host: Database host address
        port: Database port number
        database: Database name
        username: Username
        password: Password
        sql: SQL statement to execute
        params: SQL parameter dictionary (optional)

    Returns:
        If executing a query statement, returns a list where each element is a row dictionary;
        If executing a non-query statement, returns a dictionary containing the number of affected rows, e.g., {"rowcount": 3}
    """
    driver = {
        'mysql': 'pymysql',
        'oracle': 'cx_oracle',
        'sqlserver': 'pymssql',
        'hologres': 'psycopg2'
    }.get(db_type.lower(), '')

    encoded_username = quote_plus(username)
    encoded_password = quote_plus(password)
    
    # Handle Hologres type, use PostgreSQL connection method
    actual_db_type = db_type.lower()
    if actual_db_type == 'hologres':
        actual_db_type = 'postgresql'
    
    # Create database engine
    connection_url = f'{actual_db_type}+{driver}://{encoded_username}:{encoded_password}@{host}:{port}/{database}'
    
    # Add application_name parameter with version for PostgreSQL/Hologres
    if db_type.lower() == 'hologres' or db_type.lower() == 'postgresql':
        connection_url += f'?application_name=hologres_text2data_from_dify_v{APP_VERSION}'
    
    engine = create_engine(connection_url)
    
    try:
        # Use begin() to ensure transaction auto-commit
        with engine.begin() as conn:
            stmt = text(sql)
            result_proxy = conn.execute(stmt, params or {})
            # If returning row data, it's a query statement
            if result_proxy.returns_rows:
                rows = result_proxy.fetchall()
                keys = result_proxy.keys()
                return [dict(zip(keys, row)) for row in rows]
            else:
                # Non-query statement returns the number of affected rows
                return {"rowcount": result_proxy.rowcount}
    except SQLAlchemyError as e:
        raise ValueError(f"Database error: {str(e)}")
    finally:
        engine.dispose()