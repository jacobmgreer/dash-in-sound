import os
import glob
import pathlib
import pandas as pd
import duckdb
import streamlit as st

# PREREQS
# curl -LsSf https://astral.sh/uv/install.sh | sh
# SYNC DEPENDENCIES
# uv sync
# LOAD
# uv run streamlit run streamlit_app.py

# =============================================================================
# CONFIGURATION & PATH RESOLUTION
# =============================================================================

st.set_page_config(
    page_title="In-SOUNDS",
    page_icon=":nazar_amulet:",
    layout="wide",
    initial_sidebar_state="collapsed"
)

TYPE_CONTENT = 1
TYPE_CREATOR = 2
HF_DATASET_URI = "hf://datasets/jacobmgreer/in-sound/**/*.parquet"
MACRO_DIR = "macros"

def resolve_dataset_location() -> str:
    """
    Checks local disk for Parquet shards first; falls back to Hugging Face remote URI.
    """
    candidate_paths = ["data/*.parquet"]
    for path in candidate_paths:
        if glob.glob(path):
            return path
    return HF_DATASET_URI

# =============================================================================
# ENGINE INITIALIZATION & DATABASE SETUP
# =============================================================================

@st.cache_resource
def get_engine_state():
    """
    Initializes in-memory DuckDB connection, registers SQL macros, materializes
    dimension tables, auto-detects Parquet schema, and loads the dataset into RAM.
    """
    con = duckdb.connect(database=":memory:", read_only=False)
    
    # Configure network resilience and extensions
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET http_timeout = 120000;")
    con.execute("SET http_retries = 5;")
    con.execute("SET http_retry_backoff = 2.0;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("PRAGMA threads=6;")

    # 1. Register macro files if present in the macro directory
    if os.path.exists(MACRO_DIR):
        for macro_file in glob.glob(os.path.join(MACRO_DIR, "*.sql")):
            sql_text = pathlib.Path(macro_file).read_text(encoding="utf-8")
            if sql_text.strip():
                con.execute(sql_text)

    # 2. Extract bit mappings from DuckDB macros
    def load_bits(macro_name: str) -> dict:
        df = con.execute(f"SELECT bit, value FROM {macro_name}() ORDER BY bit DESC").df()
        return dict(zip(df["value"].astype(str), df["bit"].astype(int)))

    decade_bits = load_bits("get_decade_mapping")
    origin_bits = load_bits("get_origin_mapping")
    graph_bits = load_bits("get_comp_mapping")
    source_id_to_name = load_bits("get_source_mapping")

    # 3. Create dimension lookup tables
    def build_dim_table(table_name: str, bits_dict: dict, numeric_val: bool = False):
        values_sql = ", ".join(
            f"({bit}, {val if numeric_val else f'{val!r}'})"
            for val, bit in bits_dict.items()
        )
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT bit, value FROM (VALUES {values_sql}) AS t(bit, value)")

    build_dim_table("dim_decade", decade_bits, numeric_val=True)
    build_dim_table("dim_origin", origin_bits)

    source_rows = ", ".join(f"({bit}, '{name}')" for name, bit in source_id_to_name.items())
    con.execute(f"CREATE OR REPLACE TABLE dim_source AS SELECT bit, value FROM (VALUES {source_rows}) AS t(bit, value)")

    # 4. Resolve Parquet schema dynamically
    data_loc = resolve_dataset_location()
    schema_df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{data_loc}')").df()
    cols = schema_df["column_name"].tolist()

    def pick_col(candidates: list, label: str) -> str:
        for c in candidates:
            if c in cols:
                return c
        raise ValueError(f"Could not find {label}. Tried: {candidates}")

    schema = {
        "id": pick_col(["big", "id"], "ID column"),
        "type": pick_col(["type"], "Type column"),
        "decades": pick_col(["decades"], "Decade bitmask"),
        "source": pick_col(["source"], "Source column"),
        "origins": pick_col(["origins"], "Origin bitmask"),
        "graph_col": pick_col(["comp"], "Graph bitmask"),
    }

    # 5. Ingest full Parquet dataset into in-memory DuckDB table
    con.execute(f"CREATE TABLE nc_data AS SELECT * FROM read_parquet('{data_loc}');")

    # 6. Pre-calculate distinct filter choices from dataset
    def get_distinct_dim_values(bitmask_col: str, dim_table: str) -> list:
        sql = f"""
            SELECT DISTINCT d.value AS val
            FROM nc_data p
            JOIN {dim_table} d ON (COALESCE(p.{bitmask_col}, 0)::BIGINT & (1::BIGINT << d.bit)) <> 0
            ORDER BY val
        """
        return con.execute(sql).df()["val"].tolist()

    sources_df = con.execute(f"""
        SELECT DISTINCT p.{schema['source']} AS source_id,
               COALESCE(d.value, 'Source ' || CAST(p.{schema['source']} AS VARCHAR)) AS source_name
        FROM nc_data p
        LEFT JOIN dim_source d ON p.{schema['source']} = d.bit
        WHERE p.{schema['source']} IS NOT NULL
        ORDER BY source_name
    """).df()

    filter_choices = {
        "decades": [int(x) for x in get_distinct_dim_values(schema["decades"], "dim_decade")],
        "origins": [str(x) for x in get_distinct_dim_values(schema["origins"], "dim_origin")],
        "sources": dict(zip(sources_df["source_name"], sources_df["source_id"])),
    }

    return {
        "con": con,
        "schema": schema,
        "graph_bits": graph_bits,
        "decade_bits": decade_bits,
        "origin_bits": origin_bits,
        "filter_choices": filter_choices,
        "data_location": data_loc
    }

engine = get_engine_state()
con = engine["con"]
schema = engine["schema"]
graph_bits = engine["graph_bits"]
filter_choices = engine["filter_choices"]

# =============================================================================
# BITMASK SQL BUILDERS
# =============================================================================

def build_bitmask_clause(selected_values: list, bitmask_col: str, bits_dict: dict) -> str:
    """Generates bitwise AND filtering clause for multi-select bitmask values."""
    if not selected_values:
        return ""
    positions = [bits_dict[str(v)] for v in selected_values if str(v) in bits_dict]
    if not positions:
        return ""
    mask_val = sum(1 << pos for pos in positions)
    return f" AND ((COALESCE({bitmask_col}, 0)::BIGINT & {mask_val}::BIGINT) <> 0)"

def build_filter_clauses(filters: dict) -> str:
    """Translates UI widget states into parameter-safe SQL predicate strings."""
    clauses = []
    
    # Source filter
    selected_sources = filters.get("sources", [])
    if not selected_sources:
        clauses.append(" AND 1=0")
    elif len(selected_sources) < len(filter_choices["sources"]):
        src_list = ", ".join(str(int(s)) for s in selected_sources)
        clauses.append(f" AND {schema['source']} IN ({src_list})")

    # Decade range filter
    dec_from = filters.get("decade_from")
    dec_to = filters.get("decade_to")
    if dec_from and dec_to:
        if dec_from > dec_to:
            dec_from, dec_to = dec_to, dec_from
        selected_decs = [d for d in filter_choices["decades"] if dec_from <= d <= dec_to]
        clauses.append(build_bitmask_clause(selected_decs, schema["decades"], engine["decade_bits"]))

    # Categorical bitmask filters
    clauses.append(build_bitmask_clause(filters.get("origins", []), schema["origins"], engine["origin_bits"]))

    return "".join(clauses)

def graph_match_expr(graph_name: str, alias: str = None) -> str:
    prefix = f"{alias}." if alias else ""
    bit_pos = graph_bits[graph_name]
    return f"((COALESCE({prefix}{schema['graph_col']}, 0)::BIGINT & (1::BIGINT << {bit_pos})) <> 0)"

# =============================================================================
# DATA ANALYSIS & QUERY PIPELINES
# =============================================================================

@st.cache_data
def query_overview(_con, filter_clause: str) -> pd.DataFrame:
    base_expr = graph_match_expr("base")
    disc_expr = graph_match_expr("discovery")
    id_col = schema["id"]
    type_col = schema["type"]

    sql = f"""
        WITH filtered AS (
            SELECT {id_col}, {type_col}, {schema['graph_col']}
            FROM nc_data
            WHERE 1=1 {filter_clause}
        )
        SELECT
            COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1) AS total_content,
            COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1 AND {base_expr}) AS base_content,
            COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 1 AND {disc_expr}) AS disc_content,

            COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2) AS total_creator,
            COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2 AND {base_expr}) AS base_creator,
            COUNT(DISTINCT {id_col}) FILTER (WHERE {type_col} = 2 AND {disc_expr}) AS disc_creator
        FROM filtered
    """
    return con.execute(sql).df()

@st.cache_data
def query_dimension(_con, entity: str, dim_table: str, bitmask_col: str, filter_clause: str) -> pd.DataFrame:
    type_val = TYPE_CONTENT if entity == "content" else TYPE_CREATOR
    id_col = schema["id"]
    total_col = f"total_{entity}"
    base_expr = graph_match_expr("base", "f")
    disc_expr = graph_match_expr("discovery", "f")

    sql = f"""
        WITH filtered AS (
            SELECT * FROM nc_data WHERE {schema['type']} = {type_val} {filter_clause}
        )
        SELECT
            d.value AS grouping,
            COUNT(DISTINCT CASE WHEN {base_expr} THEN f.{id_col} END) AS base_matched,
            COUNT(DISTINCT CASE WHEN {disc_expr} THEN f.{id_col} END) AS disc_matched,
            COUNT(DISTINCT CASE WHEN NOT ({disc_expr}) THEN f.{id_col} END) AS unmatched,
            COUNT(DISTINCT f.{id_col}) AS {total_col}
        FROM filtered f
        JOIN {dim_table} d ON (COALESCE(f.{bitmask_col}, 0)::BIGINT & (1::BIGINT << d.bit)) <> 0
        GROUP BY d.value
        ORDER BY {total_col} DESC
    """
    return con.execute(sql).df()

@st.cache_data
def query_by_source(_con, entity: str, filter_clause: str) -> pd.DataFrame:
    type_val = TYPE_CONTENT if entity == "content" else TYPE_CREATOR
    id_col = schema["id"]
    total_col = f"total_{entity}"
    base_expr = graph_match_expr("base", "f")
    disc_expr = graph_match_expr("discovery", "f")
    src_col = schema["source"]

    sql = f"""
        WITH filtered AS (
            SELECT * FROM nc_data WHERE {schema['type']} = {type_val} {filter_clause}
        )
        SELECT
            COALESCE(s.value, 'Source ' || CAST(f.{src_col} AS VARCHAR)) AS source,
            COUNT(DISTINCT CASE WHEN {base_expr} THEN f.{id_col} END) AS base_matched,
            COUNT(DISTINCT CASE WHEN {disc_expr} THEN f.{id_col} END) AS disc_matched,
            COUNT(DISTINCT CASE WHEN NOT ({disc_expr}) THEN f.{id_col} END) AS unmatched,
            COUNT(DISTINCT f.{id_col}) AS {total_col}
        FROM filtered f
        LEFT JOIN dim_source s ON f.{src_col} = s.bit
        GROUP BY 1
        ORDER BY {total_col} DESC
    """
    return con.execute(sql).df()

# =============================================================================
# FORMATTING & PRESENTATION HELPERS
# =============================================================================

def format_summary_dataframe(df: pd.DataFrame, group_col_name: str, total_col: str) -> pd.DataFrame:
    """Formats raw count dataframes with percentage calculations and clean schema names."""
    if df.empty:
        return pd.DataFrame()

    out = pd.DataFrame()
    out[group_col_name] = df["grouping"] if "grouping" in df.columns else df[df.columns[0]]
    
    # Calculate percentages for connected graph cuts
    out["Base %"] = (100 * df["base_matched"] / df[total_col]).map("{:.1f}%".format)
    out["Base Connected"] = df["base_matched"].map("{:,}".format)

    out["Proposed %"] = (100 * df["disc_matched"] / df[total_col]).map("{:.1f}%".format)
    out["Proposed Connected"] = df["disc_matched"].map("{:,}".format)

    out["Unmatched"] = df["unmatched"].map("{:,}".format)
    out["Total"] = df[total_col].map("{:,}".format)

    return out

def render_overview_cards(df_ov: pd.DataFrame, entity_prefix: str):
    """Renders total entity count metrics alongside match rate percentages."""
    if df_ov.empty:
        st.warning("No data available for current filter selection.")
        return

    total = df_ov[f"total_{entity_prefix}"].iloc[0]
    base_m = df_ov[f"base_{entity_prefix}"].iloc[0]
    disc_m = df_ov[f"disc_{entity_prefix}"].iloc[0]

    c1, c2 = st.columns(2)
    
    def calc_pct(n, d):
        return f"{(100 * n / d):.1f}%" if d > 0 else "—"

    c1.metric(
        label="Base Graph",
        value=calc_pct(base_m, total),
        delta=f"{base_m:,} connected / {total - base_m:,} unmatched",
        delta_color="off"
    )
    c2.metric(
        label="Proposed Graph",
        value=calc_pct(disc_m, total),
        delta=f"{disc_m:,} connected / {total - disc_m:,} unmatched",
        delta_color="off"
    )

# =============================================================================
# SIDEBAR CONTROLS & FILTER WIDGETS
# =============================================================================

st.sidebar.title("Cross-Filters")

selected_decade_from = st.sidebar.selectbox(
    "From Decade",
    options=[None] + filter_choices["decades"],
    format_func=lambda x: "All" if x is None else f"{x}s"
)

selected_decade_to = st.sidebar.selectbox(
    "To Decade",
    options=[None] + filter_choices["decades"],
    format_func=lambda x: "All" if x is None else f"{x}s"
)

selected_origins = st.sidebar.multiselect("Origin(s)", options=filter_choices["origins"])

st.sidebar.markdown("---")
st.sidebar.subheader("Source Registries")

all_sources_dict = filter_choices["sources"]
selected_sources = st.sidebar.multiselect(
    "Select Sources",
    options=list(all_sources_dict.values()),
    default=list(all_sources_dict.values()),
    format_func=lambda src_id: [k for k, v in all_sources_dict.items() if v == src_id][0]
)

# Collect current filter state
current_filters = {
    "decade_from": selected_decade_from,
    "decade_to": selected_decade_to,
    "origins": selected_origins,
    "sources": selected_sources
}

filter_sql = build_filter_clauses(current_filters)

# =============================================================================
# DASHBOARD TABS & MAIN LAYOUT
# =============================================================================

st.title("🪬 SEER in-SOUNDS")
st.subheader("🆂earch 🅴ngine techniques for 🅴ntity 🆁esolution")
# st.caption(f"Data source location: `{engine['data_location']}`")

tab_source, tab_decade, tab_origin = st.tabs([
    "By Source", "By Decade", "By Origin"
])

# -----------------------------------------------------------------------------
# TAB 1: BY SOURCE
# -----------------------------------------------------------------------------
with tab_source:
    st.space(size = "large")
    st.space(size = "large")

    overview_df = query_overview(con, filter_sql)

    st.header("Content Entities")
    render_overview_cards(overview_df, "content")
    st.subheader("Content Records by Source")
    df_src_content = query_by_source(con, "content", filter_sql)
    st.dataframe(
        format_summary_dataframe(df_src_content, "Source", "total_content"),
        width="stretch"
    )

    st.markdown("---")
    st.space(size = "large")
    st.space(size = "large")

    st.header("Creator Entities")
    render_overview_cards(overview_df, "creator")
    st.subheader("Creator Records by Source")
    df_src_creator = query_by_source(con, "creator", filter_sql)
    st.dataframe(
        format_summary_dataframe(df_src_creator, "Source", "total_creator"),
        width="stretch"
    )

# -----------------------------------------------------------------------------
# TAB 2: BY DECADE
# -----------------------------------------------------------------------------
with tab_decade:
    st.space(size = "large")
    st.space(size = "large")

    st.header("Content Records by Decade")
    df_dec_content = query_dimension(con, "content", "dim_decade", schema["decades"], filter_sql)
    st.dataframe(
        format_summary_dataframe(df_dec_content, "Decade", "total_content"),
        width="stretch"
    )

    st.markdown("---")
    st.space(size = "large")
    st.space(size = "large")

    st.header("Creator Records by Associated Content Decade(s)")
    df_dec_creator = query_dimension(con, "creator", "dim_decade", schema["decades"], filter_sql)
    st.dataframe(
        format_summary_dataframe(df_dec_creator, "Decade", "total_creator"),
        width="stretch"
    )

# -----------------------------------------------------------------------------
# TAB 3: BY ORIGIN
# -----------------------------------------------------------------------------
with tab_origin:
    st.space(size = "large")
    st.space(size = "large")

    st.header("Content Records by Origin(s)")
    df_orig_content = query_dimension(con, "content", "dim_origin", schema["origins"], filter_sql)
    st.dataframe(
        format_summary_dataframe(df_orig_content, "Origin", "total_content"),
        width="stretch"
    )

    st.markdown("---")
    st.space(size = "large")
    st.space(size = "large")

    st.header("Creator Records by Associated Content Origin(s)")
    df_orig_creator = query_dimension(con, "creator", "dim_origin", schema["origins"], filter_sql)
    st.dataframe(
        format_summary_dataframe(df_orig_creator, "Origin", "total_creator"),
        width="stretch"
    )
