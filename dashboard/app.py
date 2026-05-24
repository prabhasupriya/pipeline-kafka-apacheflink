import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import os
import time
from datetime import datetime, timezone
from collections import defaultdict

st.set_page_config(page_title="MNC Feature Store Monitor", layout="wide")

# ── Clean readable CSS ───────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #f0f2f6; }
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 16px 20px;
        margin-bottom: 12px;
        border-left: 4px solid #1f77b4;
        box-shadow: 0 1px 4px rgba(0,0,0,0.1);
    }
    .metric-label {
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #555;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 26px;
        font-weight: 700;
        color: #1a1a2e;
    }
    .metric-value.green { color: #2e7d32; }
    .metric-value.orange { color: #e65100; }
    .metric-value.red { color: #c62828; }
    .fresh-card {
        background: white;
        border-radius: 8px;
        padding: 14px 16px;
        border-left: 4px solid #2e7d32;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        margin-bottom: 8px;
    }
    .stale-card {
        background: white;
        border-radius: 8px;
        padding: 14px 16px;
        border-left: 4px solid #e65100;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        margin-bottom: 8px;
    }
    .waiting-card {
        background: #f5f5f5;
        border-radius: 8px;
        padding: 14px 16px;
        border-left: 4px solid #bbb;
        margin-bottom: 8px;
    }
    .section-title {
        font-size: 14px;
        font-weight: 700;
        color: #1a1a2e;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin: 20px 0 10px 0;
        padding-bottom: 6px;
        border-bottom: 2px solid #1f77b4;
    }
    .feature-row {
        background: white;
        border-radius: 6px;
        padding: 10px 14px;
        margin-bottom: 6px;
        border-left: 3px solid #1f77b4;
        box-shadow: 0 1px 2px rgba(0,0,0,0.07);
        color: #1a1a2e;
        font-size: 14px;
    }
    .badge {
        display: inline-block;
        background: #e3f2fd;
        color: #1565c0;
        border-radius: 4px;
        padding: 2px 8px;
        font-size: 11px;
        font-weight: 600;
        margin-right: 4px;
    }
    /* Make all Streamlit text dark */
    .stMarkdown, p, span, div { color: #1a1a2e !important; }
    h1, h2, h3 { color: #1a1a2e !important; }
    .stDataFrame { background: white; }
</style>
""", unsafe_allow_html=True)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

# ── Session state ────────────────────────────────────────────────────────────
for key, default in [
    ("features", {}),
    ("late_events_count", 0),
    ("last_update", None),
    ("total_messages", 0),
]:
    if key not in st.session_state:
        st.session_state[key] = default


@st.cache_resource
def get_consumer():
    try:
        return KafkaConsumer(
            "feature-store",
            bootstrap_servers=[KAFKA_SERVERS],
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="dashboard-v3",
            consumer_timeout_ms=400,
        )
    except Exception as e:
        st.error(f"Kafka error: {e}")
        return None


def poll():
    c = get_consumer()
    if not c:
        return
    pack = c.poll(timeout_ms=400)
    for _, msgs in pack.items():
        for msg in msgs:
            v = msg.value
            if not v:
                continue
            eid = v.get("entity_id", "")
            st.session_state.features[eid] = v
            st.session_state.total_messages += 1
            st.session_state.last_update = datetime.now(timezone.utc)


def freshness(computed_at):
    try:
        ts = datetime.strptime(computed_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        secs = int((datetime.now(timezone.utc) - ts).total_seconds())
        if secs < 120:
            return f"{secs}s ago", "green"
        return f"{secs//60}m ago", "orange"
    except Exception:
        return "unknown", "orange"


poll()

# ── HEADER ───────────────────────────────────────────────────────────────────
st.markdown("# 📡 MNC Feature Store Monitor")
st.markdown(
    '<span class="badge">Kafka</span>'
    '<span class="badge">Apache Flink</span>'
    '<span class="badge">Real-Time ML</span>',
    unsafe_allow_html=True,
)

status = "🟢 **Pipeline Active**" if st.session_state.last_update else "⚪ Waiting for data..."
st.markdown(status)
st.markdown("---")

# ── METRICS ──────────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">⚙ Pipeline Health Metrics</div>', unsafe_allow_html=True)

m1, m2, m3, m4 = st.columns(4)

with m1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Total Features Computed</div>
        <div class="metric-value green">{st.session_state.total_messages:,}</div>
    </div>""", unsafe_allow_html=True)

with m2:
    late = st.session_state.late_events_count
    cls = "orange" if late > 0 else "green"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Late Events Dropped</div>
        <div class="metric-value {cls}">{late}</div>
    </div>""", unsafe_allow_html=True)

with m3:
    if st.session_state.last_update:
        lag = int((datetime.now(timezone.utc) - st.session_state.last_update).total_seconds())
        lag_str = f"~{lag}s"
        lag_cls = "red" if lag > 120 else "orange" if lag > 30 else "green"
    else:
        lag_str, lag_cls = "—", "orange"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Watermark Lag</div>
        <div class="metric-value {lag_cls}">{lag_str}</div>
    </div>""", unsafe_allow_html=True)

with m4:
    lu = st.session_state.last_update
    lu_str = lu.strftime("%H:%M:%S") if lu else "—"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Last Feature Update</div>
        <div class="metric-value" style="font-size:20px;color:#1a1a2e">{lu_str}</div>
    </div>""", unsafe_allow_html=True)

# ── FEATURE FRESHNESS ────────────────────────────────────────────────────────
st.markdown('<div class="section-title">🕐 Feature Freshness Monitor</div>', unsafe_allow_html=True)

TRACKED = ["click_rate", "avg_dwell_time", "engagement_rate", "category_affinity_score"]
fcols = st.columns(len(TRACKED))

for i, fname in enumerate(TRACKED):
    last_seen = None
    for eid, rec in st.session_state.features.items():
        if rec.get("feature_name") == fname:
            if last_seen is None or rec["computed_at"] > last_seen:
                last_seen = rec["computed_at"]

    with fcols[i]:
        if last_seen:
            age_str, cls = freshness(last_seen)
            color = "#2e7d32" if cls == "green" else "#e65100"
            st.markdown(f"""
            <div class="{'fresh-card' if cls == 'green' else 'stale-card'}">
                <div class="metric-label">{fname}</div>
                <div style="font-size:20px;font-weight:700;color:{color}">{age_str}</div>
                <div style="font-size:11px;color:#666;margin-top:4px">{last_seen}</div>
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="waiting-card">
                <div class="metric-label">{fname}</div>
                <div style="font-size:18px;color:#999">—</div>
                <div style="font-size:11px;color:#aaa;margin-top:4px">awaiting data</div>
            </div>""", unsafe_allow_html=True)

# ── ENTITY VIEWER ────────────────────────────────────────────────────────────
st.markdown('<div class="section-title">🔍 Entity Feature Viewer</div>', unsafe_allow_html=True)

col_in, col_btn = st.columns([4, 1])
with col_in:
    query = st.text_input("Enter user_id or content_id", placeholder="e.g. u001 or c001",
                          label_visibility="collapsed")
with col_btn:
    st.button("Search")

if query:
    matches = {eid: rec for eid, rec in st.session_state.features.items()
               if query.strip().lower() in eid.lower()}
    if matches:
        st.success(f"Found {len(matches)} feature(s) for `{query}`")
        for eid, rec in sorted(matches.items()):
            age_str, cls = freshness(rec["computed_at"])
            color = "#2e7d32" if cls == "green" else "#e65100"
            st.markdown(f"""
            <div class="feature-row">
                <strong style="color:#1565c0">{eid}</strong><br>
                <span style="color:#333">{rec['feature_name']}</span>
                &nbsp;=&nbsp;
                <strong style="color:#1a1a2e;font-size:15px">{rec['feature_value']}</strong>
                <span style="float:right;color:{color};font-size:12px">
                    {age_str} · {rec['computed_at']}
                </span>
            </div>""", unsafe_allow_html=True)
    else:
        st.warning(f"No features found for `{query}`. Try u001, u002, c001, c002...")

# ── ALL FEATURES TABLE ───────────────────────────────────────────────────────
st.markdown('<div class="section-title">📊 All Latest Feature Values</div>', unsafe_allow_html=True)

if st.session_state.features:
    rows = []
    for eid, rec in st.session_state.features.items():
        age_str, _ = freshness(rec.get("computed_at", ""))
        rows.append({
            "Entity ID": eid,
            "Feature": rec.get("feature_name", ""),
            "Value": rec.get("feature_value", ""),
            "Computed At": rec.get("computed_at", ""),
            "Freshness": age_str,
        })
    df = pd.DataFrame(rows).sort_values("Computed At", ascending=False)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("⏳ Waiting for Flink to compute features... Check localhost:8081")

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(f"Tracking {len(st.session_state.features)} entities · Refreshes every 2s · Kafka: {KAFKA_SERVERS}")

time.sleep(2)
st.rerun()