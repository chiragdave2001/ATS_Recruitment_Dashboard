import streamlit as st
import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

st.set_page_config(page_title="ATS Recruitment Dashboard", layout="wide")

DB_SCHEMA = st.secrets["postgres"].get("schema", "ats_dwh")

COLORS = ["#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
          "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC"]

def get_connection():
    return psycopg2.connect(
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        host=st.secrets["postgres"].get("host", "localhost"),
        port=st.secrets["postgres"].get("port", 5432),
        sslmode=st.secrets["postgres"].get("sslmode", "prefer"),
        connect_timeout=10
    )

@st.cache_data(ttl=600)
def run_query(sql):
    """Execute query with timeout and proper error handling"""
    try:
        conn = get_connection()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(f"SET statement_timeout TO '30s'")
            cur.execute(sql)
            result = cur.fetchall()
            columns = [desc[0].upper() for desc in cur.description]
        finally:
            cur.close()
            conn.close()

        if not result:
            return pd.DataFrame()
        return pd.DataFrame(result, columns=columns)
    except Exception as e:
        st.error(f"❌ Query Error: {str(e)[:200]}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def run_queries(query_map):
    """Fetch multiple query results on a single connection and return a dict of DataFrames."""
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SET statement_timeout TO '30s'")
        results = {}
        for key, sql in query_map.items():
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [desc[0].upper() for desc in cur.description]
            results[key] = pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        st.error(f"❌ Query Error: {str(e)[:200]}")
        return {key: pd.DataFrame() for key in query_map.keys()}


def plot_bar(data, x_col, y_col, title):
    if data.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.barh(data[x_col].astype(str), data[y_col], color=COLORS[0])
    ax.set_xlabel(y_col)
    ax.set_title(title)
    st.pyplot(fig, width='stretch')
    plt.close()

def plot_pie(data, label_col, value_col, title):
    if data.empty:
        return
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.pie(data[value_col], labels=data[label_col], autopct='%1.1f%%', colors=COLORS)
    ax.set_title(title)
    st.pyplot(fig, width='stretch')
    plt.close()

st.title("🎯 ATS Recruitment Dashboard")
st.markdown("Professional Recruitment Analytics & Insights")

page = st.sidebar.radio("📊 Navigation", [
    "Overview", "Hiring Pipeline", "Interview Analytics", 
    "Recruiter Performance", "Candidate Analytics", "Job Analytics"
])

if page == "Overview":
    st.header("📈 Overview")

    overview_queries = {
        "total_applications": f"SELECT COUNT(*) AS cnt FROM {DB_SCHEMA}.fact_application",
        "total_interviews": f"SELECT COUNT(*) AS cnt FROM {DB_SCHEMA}.fact_interview",
        "total_hires": f"SELECT COUNT(*) AS cnt FROM {DB_SCHEMA}.fact_application fa JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key WHERE das.is_current = TRUE AND das.application_status_name = 'Selected'",
        "active_candidates": f"SELECT COUNT(*) AS cnt FROM {DB_SCHEMA}.dim_candidate WHERE is_current = TRUE",
        "status_distribution": f"""
            SELECT das.application_status_name, COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key
            WHERE das.is_current = TRUE
            GROUP BY das.application_status_name
            ORDER BY cnt DESC
            LIMIT 10
        """,
        "top_departments": f"""
            SELECT dd.department_name, COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_job dj ON fa.job_key = dj.job_key
            JOIN {DB_SCHEMA}.dim_department dd ON dj.department_id = dd.department_id
            WHERE dd.is_current = TRUE
            GROUP BY dd.department_name
            ORDER BY cnt DESC
            LIMIT 10
        """
    }

    with st.spinner("Loading overview data..."):
        overview_data = run_queries(overview_queries)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        val = overview_data["total_applications"]["CNT"].iloc[0] if not overview_data["total_applications"].empty else 0
        st.metric("Total Applications", f"{int(val):,}")
    with col2:
        val = overview_data["total_interviews"]["CNT"].iloc[0] if not overview_data["total_interviews"].empty else 0
        st.metric("Total Interviews", f"{int(val):,}")
    with col3:
        val = overview_data["total_hires"]["CNT"].iloc[0] if not overview_data["total_hires"].empty else 0
        st.metric("Total Hires", f"{int(val):,}")
    with col4:
        val = overview_data["active_candidates"]["CNT"].iloc[0] if not overview_data["active_candidates"].empty else 0
        st.metric("Active Candidates", f"{int(val):,}")

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Application Status")
        if not overview_data["status_distribution"].empty:
            plot_pie(overview_data["status_distribution"], "APPLICATION_STATUS_NAME", "CNT", "Status Distribution")
    with col2:
        st.subheader("Top Departments")
        if not overview_data["top_departments"].empty:
            plot_bar(overview_data["top_departments"], "DEPARTMENT_NAME", "CNT", "Applications by Department")

elif page == "Hiring Pipeline":
    st.header("🔄 Hiring Pipeline")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_application")
        st.metric("Total Applications", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col2:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_interview")
        st.metric("Total Interviews", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col3:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_application fa JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key WHERE das.is_current = TRUE AND das.application_status_name = 'Selected'")
        st.metric("Total Hires", result['CNT'].iloc[0] if not result.empty else 0)
    
    st.divider()
    st.subheader("Applications by Status")
    result = run_query(f"""
        SELECT das.application_status_name, COUNT(*) as cnt
        FROM {DB_SCHEMA}.fact_application fa
        JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key
        WHERE das.is_current = TRUE
        GROUP BY das.application_status_name
        ORDER BY cnt DESC
    """)
    if not result.empty:
        st.dataframe(result, use_container_width=True, hide_index=True)

elif page == "Interview Analytics":
    st.header("🎤 Interview Analytics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_interview")
        st.metric("Total Interviews", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col2:
        result = run_query(f"""
            SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_interview fi
            JOIN {DB_SCHEMA}.dim_interview_status dis ON fi.interview_status_key = dis.interview_status_key
            WHERE dis.is_current = TRUE AND dis.interview_status_name = 'Scheduled'
        """)
        st.metric("Scheduled", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col3:
        result = run_query(f"""
            SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_interview fi
            JOIN {DB_SCHEMA}.dim_interview_status dis ON fi.interview_status_key = dis.interview_status_key
            WHERE dis.is_current = TRUE AND dis.interview_status_name = 'Completed'
        """)
        st.metric("Completed", result['CNT'].iloc[0] if not result.empty else 0)
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Interview Types")
        result = run_query(f"""
            SELECT dit.interview_type_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.fact_interview fi
            JOIN {DB_SCHEMA}.dim_interview_type dit ON fi.interview_type_key = dit.interview_type_key
            WHERE dit.is_current = TRUE
            GROUP BY dit.interview_type_name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        if not result.empty:
            plot_bar(result, "INTERVIEW_TYPE_NAME", "CNT", "Interview Types")
    
    with col2:
        st.subheader("Interview Status")
        result = run_query(f"""
            SELECT dis.interview_status_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.fact_interview fi
            JOIN {DB_SCHEMA}.dim_interview_status dis ON fi.interview_status_key = dis.interview_status_key
            WHERE dis.is_current = TRUE
            GROUP BY dis.interview_status_name
            ORDER BY cnt DESC
        """)
        if not result.empty:
            plot_pie(result, "INTERVIEW_STATUS_NAME", "CNT", "Status Distribution")

elif page == "Recruiter Performance":
    st.header("👥 Recruiter Performance")
    
    st.subheader("Top Recruiters by Applications")
    result = run_query(f"""
        SELECT 
            dr.recruiter_name,
            COUNT(*) as applications
        FROM {DB_SCHEMA}.fact_application fa
        JOIN {DB_SCHEMA}.dim_recruiter dr ON fa.recruiter_key = dr.recruiter_key
        WHERE dr.is_current = TRUE
        GROUP BY dr.recruiter_name
        ORDER BY applications DESC
        LIMIT 15
    """)
    if not result.empty:
        st.dataframe(result, use_container_width=True, hide_index=True)
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Applications by Recruiter")
        result = run_query(f"""
            SELECT dr.recruiter_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_recruiter dr ON fa.recruiter_key = dr.recruiter_key
            WHERE dr.is_current = TRUE
            GROUP BY dr.recruiter_name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        if not result.empty:
            plot_bar(result, "RECRUITER_NAME", "CNT", "Top Recruiters")
    
    with col2:
        st.subheader("Hires by Recruiter")
        result = run_query(f"""
            SELECT dr.recruiter_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_recruiter dr ON fa.recruiter_key = dr.recruiter_key
            JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key
            WHERE dr.is_current = TRUE AND das.is_current = TRUE AND das.application_status_name = 'Selected'
            GROUP BY dr.recruiter_name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        if not result.empty:
            plot_bar(result, "RECRUITER_NAME", "CNT", "Top Hires")

elif page == "Candidate Analytics":
    st.header("👨‍💼 Candidate Analytics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.dim_candidate WHERE is_current = TRUE")
        st.metric("Total Candidates", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col2:
        result = run_query(f"SELECT COUNT(DISTINCT fa.candidate_key) as cnt FROM {DB_SCHEMA}.fact_application fa JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key WHERE das.is_current = TRUE AND das.application_status_name = 'Selected'")
        st.metric("Hired", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col3:
        result = run_query(f"""
            SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key
            WHERE das.is_current = TRUE AND das.application_status_name = 'Rejected'
        """)
        st.metric("Rejected", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col4:
        st.metric("Active Profile Status", "—")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Candidates by Gender")
        result = run_query(f"""
            SELECT dg.gender_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.dim_candidate dc
            LEFT JOIN {DB_SCHEMA}.dim_gender dg ON dc.gender_id = dg.gender_id
            WHERE dc.is_current = TRUE
            GROUP BY dg.gender_name
            ORDER BY cnt DESC
        """)
        if not result.empty:
            plot_pie(result, "GENDER_NAME", "CNT", "By Gender")
    
    with col2:
        st.subheader("Employment Type")
        result = run_query(f"""
            SELECT det.employment_type_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.dim_candidate dc
            LEFT JOIN {DB_SCHEMA}.dim_employment_type det ON dc.employment_type_id = det.employment_type_id
            WHERE dc.is_current = TRUE
            GROUP BY det.employment_type_name
            ORDER BY cnt DESC
        """)
        if not result.empty:
            plot_pie(result, "EMPLOYMENT_TYPE_NAME", "CNT", "By Type")

elif page == "Job Analytics":
    st.header("💼 Job Analytics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.dim_job WHERE is_current = TRUE")
        st.metric("Total Jobs", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col2:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_application")
        st.metric("Applications", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col3:
        result = run_query(f"SELECT COUNT(*) as cnt FROM {DB_SCHEMA}.fact_application fa JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key WHERE das.is_current = TRUE AND das.application_status_name = 'Selected'")
        st.metric("Hires", result['CNT'].iloc[0] if not result.empty else 0)
    
    with col4:
        st.metric("Active Positions", "—")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Jobs by Department")
        result = run_query(f"""
            SELECT dd.department_name, COUNT(*) AS cnt
            FROM {DB_SCHEMA}.dim_job dj
            JOIN {DB_SCHEMA}.dim_department dd ON dj.department_id = dd.department_id
            WHERE dj.is_current = TRUE AND dd.is_current = TRUE
            GROUP BY dd.department_name
            ORDER BY cnt DESC
            LIMIT 10
        """)
        if not result.empty:
            plot_bar(result, "DEPARTMENT_NAME", "CNT", "Jobs by Department")
    
    with col2:
        st.subheader("Jobs by Work Mode")
        result = run_query(f"""
            SELECT dwm.work_mode_name, COUNT(*) as cnt
            FROM {DB_SCHEMA}.dim_job dj
            LEFT JOIN {DB_SCHEMA}.dim_work_mode dwm ON dj.work_mode_id = dwm.work_mode_id
            WHERE dj.is_current = TRUE
            GROUP BY dwm.work_mode_name
            ORDER BY cnt DESC
        """)
        if not result.empty:
            plot_pie(result, "WORK_MODE_NAME", "CNT", "By Work Mode")
    
    st.divider()
    st.subheader("Job Details")
    result = run_query(f"""
        SELECT 
            dj.job_title,
            dd.designation_name,
            dept.department_name
        FROM {DB_SCHEMA}.dim_job dj
        LEFT JOIN {DB_SCHEMA}.dim_designation dd ON dj.designation_id = dd.designation_id
        LEFT JOIN {DB_SCHEMA}.dim_department dept ON dj.department_id = dept.department_id
        WHERE dj.is_current = TRUE
        LIMIT 15
    """)
    if not result.empty:
        st.dataframe(result, use_container_width=True, hide_index=True)

st.divider()
st.markdown(f"*Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
