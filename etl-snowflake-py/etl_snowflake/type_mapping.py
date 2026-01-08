"""PostgreSQL to Snowflake type mapping."""

from typing import Dict, Tuple

# PostgreSQL OID to Snowflake type mapping
# Based on PostgreSQL type OIDs from pg_type catalog
POSTGRES_TO_SNOWFLAKE: Dict[int, str] = {
    # Boolean
    16: "BOOLEAN",      # bool
    
    # Integer types
    21: "SMALLINT",     # int2
    23: "INTEGER",      # int4
    20: "BIGINT",       # int8
    
    # Floating point
    700: "FLOAT",       # float4
    701: "DOUBLE",      # float8
    
    # Numeric/Decimal
    1700: "NUMBER(38,10)",  # numeric
    
    # Character types
    18: "CHAR(1)",      # char
    25: "VARCHAR",      # text
    1042: "VARCHAR",    # bpchar
    1043: "VARCHAR",    # varchar
    19: "VARCHAR(63)",  # name
    
    # Date/Time types
    1082: "DATE",           # date
    1083: "TIME",           # time
    1114: "TIMESTAMP_NTZ",  # timestamp
    1184: "TIMESTAMP_TZ",   # timestamptz
    1186: "VARCHAR",        # interval (no direct equivalent)
    
    # UUID
    2950: "VARCHAR(36)",    # uuid
    
    # JSON types
    114: "VARIANT",     # json
    3802: "VARIANT",    # jsonb
    
    # Binary
    17: "BINARY",       # bytea
    
    # Network types (store as text)
    869: "VARCHAR",     # inet
    650: "VARCHAR",     # cidr
    829: "VARCHAR",     # macaddr
    
    # Geometric types
    600: "VARCHAR",     # point
    601: "VARCHAR",     # lseg
    602: "VARCHAR",     # path
    603: "VARCHAR",     # box
    604: "VARCHAR",     # polygon
    718: "VARCHAR",     # circle
    
    # PostGIS geometry (use GEOGRAPHY if available)
    # OID varies by installation, handle by type name
    
    # OID type
    26: "INTEGER",      # oid
    
    # Array types - mapped to ARRAY in Snowflake
    1000: "ARRAY",      # bool[]
    1005: "ARRAY",      # int2[]
    1007: "ARRAY",      # int4[]
    1016: "ARRAY",      # int8[]
    1021: "ARRAY",      # float4[]
    1022: "ARRAY",      # float8[]
    1015: "ARRAY",      # varchar[]
    1009: "ARRAY",      # text[]
    1182: "ARRAY",      # date[]
    1115: "ARRAY",      # timestamp[]
    1185: "ARRAY",      # timestamptz[]
    2951: "ARRAY",      # uuid[]
    199: "ARRAY",       # json[]
    3807: "ARRAY",      # jsonb[]
    1231: "ARRAY",      # numeric[]
}

# Type name to Snowflake mapping (fallback for custom types)
POSTGRES_NAME_TO_SNOWFLAKE: Dict[str, str] = {
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "int2": "SMALLINT",
    "smallint": "SMALLINT",
    "int4": "INTEGER",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int8": "BIGINT",
    "bigint": "BIGINT",
    "float4": "FLOAT",
    "real": "FLOAT",
    "float8": "DOUBLE",
    "double precision": "DOUBLE",
    "numeric": "NUMBER(38,10)",
    "decimal": "NUMBER(38,10)",
    "text": "VARCHAR",
    "varchar": "VARCHAR",
    "character varying": "VARCHAR",
    "char": "CHAR(1)",
    "character": "CHAR(1)",
    "bpchar": "VARCHAR",
    "date": "DATE",
    "time": "TIME",
    "time without time zone": "TIME",
    "time with time zone": "TIME",
    "timestamp": "TIMESTAMP_NTZ",
    "timestamp without time zone": "TIMESTAMP_NTZ",
    "timestamptz": "TIMESTAMP_TZ",
    "timestamp with time zone": "TIMESTAMP_TZ",
    "uuid": "VARCHAR(36)",
    "json": "VARIANT",
    "jsonb": "VARIANT",
    "bytea": "BINARY",
    "geometry": "GEOGRAPHY",
    "geography": "GEOGRAPHY",
    "point": "VARCHAR",
    "line": "VARCHAR",
    "lseg": "VARCHAR",
    "box": "VARCHAR",
    "path": "VARCHAR",
    "polygon": "VARCHAR",
    "circle": "VARCHAR",
    "inet": "VARCHAR",
    "cidr": "VARCHAR",
    "macaddr": "VARCHAR",
    "oid": "INTEGER",
    "interval": "VARCHAR",
}


def postgres_to_snowflake_type(
    type_oid: int,
    type_name: str = "",
    modifier: int = -1
) -> str:
    """Convert PostgreSQL type to Snowflake type.
    
    Args:
        type_oid: PostgreSQL type OID
        type_name: PostgreSQL type name (fallback)
        modifier: Type modifier (e.g., varchar length)
    
    Returns:
        Snowflake type string
    """
    # Try OID lookup first
    if type_oid in POSTGRES_TO_SNOWFLAKE:
        snowflake_type = POSTGRES_TO_SNOWFLAKE[type_oid]
        
        # Handle varchar/char with modifier (length)
        if modifier > 0 and snowflake_type == "VARCHAR":
            # Modifier includes 4-byte header in PostgreSQL
            length = modifier - 4
            if length > 0:
                return f"VARCHAR({length})"
        
        # Handle numeric with precision/scale from modifier for OID 1700
        if type_oid == 1700 and modifier > 0:
            precision = ((modifier - 4) >> 16) & 0xFFFF
            scale = (modifier - 4) & 0xFFFF
            if precision > 0:
                # Snowflake maximum precision is 38
                precision = min(precision, 38)
                return f"NUMBER({precision},{scale})"
        
        return snowflake_type
    
    # Fallback to type name lookup
    type_name_lower = type_name.lower().strip()
    
    # Check for array types
    if type_name_lower.endswith("[]"):
        return "ARRAY"
    
    # Handle numeric with precision/scale from modifier
    if type_name_lower in ("numeric", "decimal") and modifier > 0:
        # PostgreSQL stores precision in upper 16 bits, scale in lower 16
        precision = ((modifier - 4) >> 16) & 0xFFFF
        scale = (modifier - 4) & 0xFFFF
        if precision > 0:
            precision = min(precision, 38)
            return f"NUMBER({precision},{scale})"
    
    if type_name_lower in POSTGRES_NAME_TO_SNOWFLAKE:
        return POSTGRES_NAME_TO_SNOWFLAKE[type_name_lower]
    
    # Default fallback
    return "VARCHAR"


def get_snowflake_column_def(
    column_name: str,
    type_oid: int,
    type_name: str = "",
    modifier: int = -1,
    nullable: bool = True,
    is_primary_key: bool = False
) -> str:
    """Generate Snowflake column definition.
    
    Args:
        column_name: Column name
        type_oid: PostgreSQL type OID
        type_name: PostgreSQL type name
        modifier: Type modifier
        nullable: Whether column allows NULL
        is_primary_key: Whether column is primary key
    
    Returns:
        Snowflake column definition string (e.g., '"id" INTEGER NOT NULL')
    """
    snowflake_type = postgres_to_snowflake_type(type_oid, type_name, modifier)
    
    parts = [f'"{column_name}"', snowflake_type]
    
    if not nullable:
        parts.append("NOT NULL")
    
    return " ".join(parts)
