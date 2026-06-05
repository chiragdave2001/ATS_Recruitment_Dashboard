import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np
import hashlib

st.set_page_config(page_title="ATS Recruitment Dashboard", layout="wide", initial_sidebar_state="expanded")

# Database configuration - DWH Schema
DB_SCHEMA = st.secrets["postgres"].get("schema", "ats_dwh")

# Color Palette
COLORS = ["#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
          "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC"]
PRIMARY_COLOR = "#4E79A7"
SUCCESS_COLOR = "#59A14F"
WARNING_COLOR = "#F28E2B"
DANGER_COLOR = "#E15759"

@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        host=st.secrets["postgres"].get("host", "localhost"),
        port=st.secrets["postgres"].get("port", 5432),
        sslmode=st.secrets["postgres"].get("sslmode", "prefer"),
    )
    conn.autocommit = True
    return conn

@st.cache_data(ttl=600, show_spinner=False)
def run_query(sql, query_name=""):
    """Execute query with proper caching"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql)
        columns = [desc[0].upper() for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

def safe_query(sql, use_spinner=True):
    """Execute query with error handling"""
    try:
        with st.spinner("Loading data...") if use_spinner else st.empty():
            df = run_query(sql)
            return df if not df.empty else pd.DataFrame()
    except Exception as e:
        st.error(f"Error: {e}")
        return pd.DataFrame()

def metric_card(label, value, delta=None):
    st.metric(label=label, value=value, delta=delta)

def style_ax(ax, title=None, xlabel=None, ylabel=None):
    ax.set_facecolor("none")
    ax.figure.patch.set_alpha(0.0)
    ax.patch.set_alpha(0.0)
    ax.tick_params(labelsize=9)
    if title:
        ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=10)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=10)
    ax.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.4)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

def plot_bar(data, x_col, y_col, title, color=PRIMARY_COLOR, horizontal=False, figsize=(8, 4)):
    if data.empty:
        st.info("No data to display")
        return
    data = data.copy()
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce").fillna(0)
    labels = data[x_col].astype(str).tolist()
    values = [float(v) for v in data[y_col]]
    fig, ax = plt.subplots(figsize=figsize)
    if horizontal:
        y_pos = list(range(len(labels)))
        bars = ax.barh(y_pos, values, color=color, edgecolor="none", height=0.6)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=8)
        ax.invert_yaxis()
        max_val = max(values) if values else 1
        for bar, val in zip(bars, values):
            ax.text(bar.get_width() + max_val * 0.02, bar.get_y() + bar.get_height() / 2,
                    f"{val:,.0f}", va="center", ha="left", fontsize=8)
        style_ax(ax, title)
    else:
        bars = ax.bar(range(len(labels)), values, color=color, edgecolor="none", width=0.6)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=8)
        max_val = max(values) if values else 1
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max_val * 0.02,
                    f"{val:,.0f}", ha="center", va="bottom", fontsize=7)
        style_ax(ax, title)
    fig.tight_layout()
    st.pyplot(fig, transparent=True, use_container_width=True)
    plt.close(fig)

def plot_pie(data, label_col, value_col, title, figsize=(6, 4)):
    if data.empty:
        st.info("No data to display")
        return
    data = data.copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce").fillna(0)
    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_alpha(0.0)
    ax.set_facecolor("none")
    wedges, texts, autotexts = ax.pie(
        data[value_col].tolist(), labels=data[label_col].astype(str).tolist(),
        autopct="%1.1f%%", colors=COLORS[:len(data)], startangle=140, pctdistance=0.8,
        textprops={"fontsize": 8}
    )
    for t in autotexts:
        t.set_fontsize(7)
    ax.set_title(title, fontsize=12, fontweight="bold", pad=12)
    fig.tight_layout()
    st.pyplot(fig, transparent=True, use_container_width=True)
    plt.close(fig)

def plot_line(data, x_col, y_col, title, color=PRIMARY_COLOR, figsize=(10, 4)):
    if data.empty:
        st.info("No data to display")
        return
    data = data.copy()
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce").fillna(0)
    labels = data[x_col].astype(str).tolist()
    values = [float(v) for v in data[y_col]]
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(labels, values, color=color, marker="o", linewidth=2, markersize=5)
    ax.fill_between(range(len(labels)), values, alpha=0.15, color=color)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=8)
    style_ax(ax, title)
    fig.tight_layout()
    st.pyplot(fig, transparent=True, use_container_width=True)
    plt.close(fig)

# Dashboard Title and Navigation
st.title("🎯 ATS Recruitment Dashboard")
st.markdown("Professional Recruitment Analytics & Insights")

page = st.sidebar.radio(
    "📊 Navigation",
    ["Overview", "Hiring Pipeline", "Interview Analytics", "Recruiter Performance", "Candidate Analytics", "Job Analytics"]
)

# ==================== OVERVIEW PAGE ====================
if page == "Overview":
    st.header("📈 Overview")
    
    # Load all KPIs at once to reduce queries
    with st.spinner("Loading KPIs..."):
        kpi_query = f"""
            SELECT 
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_application) AS total_applications,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_interview) AS total_interviews,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_hire) AS total_hires,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.dim_candidate WHERE is_current = TRUE) AS active_candidates
        """
        kpi_data = run_query(kpi_query)
    
    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    
    if not kpi_data.empty:
        row = kpi_data.iloc[0]
        with col1:
            metric_card("Total Applications", f"{int(row['TOTAL_APPLICATIONS']):,}")
        with col2:
            metric_card("Total Interviews", f"{int(row['TOTAL_INTERVIEWS']):,}")
        with col3:
            metric_card("Total Hires", f"{int(row['TOTAL_HIRES']):,}")
        with col4:
            metric_card("Active Candidates", f"{int(row['ACTIVE_CANDIDATES']):,}")
    
    st.divider()
    
    # Use tabs for better organization
    tab1, tab2, tab3 = st.tabs(["Trends", "Distribution", "By Source"])
    
    with tab1:
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("📅 Applications Over Time")
            apps_trend = safe_query(f"""
                SELECT 
                    d.month_name,
                    COUNT(*) AS application_count
                FROM {DB_SCHEMA}.fact_application fa
                JOIN {DB_SCHEMA}.dim_date d ON fa.application_date_key = d.date_key
                WHERE d.year_number = EXTRACT(YEAR FROM CURRENT_DATE)
                GROUP BY d.month_number, d.month_name
                ORDER BY d.month_number
                LIMIT 12
            """, use_spinner=False)
            if not apps_trend.empty:
                plot_line(apps_trend, "MONTH_NAME", "APPLICATION_COUNT", "Monthly Applications", color=PRIMARY_COLOR)
        
        with col_right:
            st.subheader("🎓 Hires Over Time")
            hires_trend = safe_query(f"""
                SELECT 
                    d.month_name,
                    COUNT(*) AS hire_count
                FROM {DB_SCHEMA}.fact_hire fh
                JOIN {DB_SCHEMA}.dim_date d ON fh.hire_date_key = d.date_key
                WHERE d.year_number = EXTRACT(YEAR FROM CURRENT_DATE)
                GROUP BY d.month_number, d.month_name
                ORDER BY d.month_number
                LIMIT 12
            """, use_spinner=False)
            if not hires_trend.empty:
                plot_line(hires_trend, "MONTH_NAME", "HIRE_COUNT", "Monthly Hires", color=SUCCESS_COLOR)
    
    with tab2:
        col_pie, col_bar = st.columns(2)
        
        status_dist = safe_query(f"""
            SELECT 
                das.application_status_name AS status,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_application_status das 
                ON fa.application_status_key = das.application_status_key
            WHERE das.is_current = TRUE
            GROUP BY das.application_status_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        
        with col_pie:
            if not status_dist.empty:
                plot_pie(status_dist, "STATUS", "CNT", "Status Distribution")
        with col_bar:
            if not status_dist.empty:
                plot_bar(status_dist, "STATUS", "CNT", "Counts by Status", color=COLORS[2], horizontal=True)
    
    with tab3:
        col_dept, col_source = st.columns(2)
        
        with col_dept:
            st.subheader("🏢 Applications by Department")
            dept_data = safe_query(f"""
                SELECT 
                    dd.department_name,
                    COUNT(*) AS cnt
                FROM {DB_SCHEMA}.fact_application fa
                JOIN {DB_SCHEMA}.dim_job dj ON fa.job_key = dj.job_key
                JOIN {DB_SCHEMA}.dim_department dd ON dj.department_key = dd.department_key
                WHERE dd.is_current = TRUE
                GROUP BY dd.department_name
                ORDER BY cnt DESC
                LIMIT 10
            """, use_spinner=False)
            if not dept_data.empty:
                plot_bar(dept_data, "DEPARTMENT_NAME", "CNT", "", color=COLORS[0], horizontal=True)
        
        with col_source:
            st.subheader("📌 Applications by Source")
            source_data = safe_query(f"""
                SELECT 
                    dps.profile_source_name,
                    COUNT(*) AS cnt
                FROM {DB_SCHEMA}.fact_application fa
                JOIN {DB_SCHEMA}.dim_profile_source dps ON fa.profile_source_key = dps.profile_source_key
                WHERE dps.is_current = TRUE
                GROUP BY dps.profile_source_name
                ORDER BY cnt DESC
                LIMIT 10
            """, use_spinner=False)
            if not source_data.empty:
                plot_bar(source_data, "PROFILE_SOURCE_NAME", "CNT", "", color=COLORS[1], horizontal=True)

# ==================== HIRING PIPELINE PAGE ====================
elif page == "Hiring Pipeline":
    st.header("🔄 Hiring Pipeline")
    
    with st.spinner("Loading pipeline data..."):
        pipeline_stats = safe_query(f"""
            SELECT 
                das.application_status_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key
            WHERE das.is_current = TRUE
            GROUP BY das.application_status_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
    
    st.subheader("Pipeline Status Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    if not pipeline_stats.empty:
        total_apps_p = pipeline_stats["CNT"].sum()
        status_map = pipeline_stats.set_index('APPLICATION_STATUS_NAME')['CNT'].to_dict()
        
        with col1:
            metric_card("Total", status_map.get("Applied", 0))
        with col2:
            metric_card("In Progress", status_map.get("In Progress", 0))
        with col3:
            metric_card("Rejected", status_map.get("Rejected", 0))
        with col4:
            metric_card("Selected", status_map.get("Selected", 0))
    
    st.divider()
    
    tab1, tab2 = st.tabs(["Funnel", "Jobs"])
    
    with tab1:
        col_funnel, col_stats = st.columns([1.5, 1])
        
        with col_funnel:
            funnel_data = pd.DataFrame({
                'STAGE': ['Applied', 'Interviews', 'Hired'],
                'CNT': [
                    kpi_data.iloc[0]['TOTAL_APPLICATIONS'] if (kpi_data := safe_query(f"SELECT COUNT(*) AS TOTAL_APPLICATIONS FROM {DB_SCHEMA}.fact_application", use_spinner=False)) and not kpi_data.empty else 0,
                    (interviews := safe_query(f"SELECT COUNT(*) AS CNT FROM {DB_SCHEMA}.fact_interview", use_spinner=False))["CNT"].iloc[0] if not interviews.empty else 0,
                    (hires := safe_query(f"SELECT COUNT(*) AS CNT FROM {DB_SCHEMA}.fact_hire", use_spinner=False))["CNT"].iloc[0] if not hires.empty else 0,
                ]
            })
            
            if not funnel_data.empty and funnel_data["CNT"].sum() > 0:
                plot_bar(funnel_data, "STAGE", "CNT", "Recruitment Funnel", color=COLORS[0])
        
        with col_stats:
            st.metric("Applications", funnel_data["CNT"].iloc[0] if len(funnel_data) > 0 else 0)
            st.metric("Interviews", funnel_data["CNT"].iloc[1] if len(funnel_data) > 1 else 0)
            st.metric("Hires", funnel_data["CNT"].iloc[2] if len(funnel_data) > 2 else 0)
            total = funnel_data["CNT"].iloc[0] if len(funnel_data) > 0 and funnel_data["CNT"].iloc[0] > 0 else 0
            conv = (funnel_data["CNT"].iloc[1] / total * 100) if total > 0 else 0
            st.metric("Conversion", f"{conv:.1f}%")
    
    with tab2:
        st.subheader("💼 Top Jobs")
        job_analysis = safe_query(f"""
            SELECT 
                dj.job_title,
                COUNT(DISTINCT fa.application_key) AS applications,
                COUNT(DISTINCT fh.hire_key) AS hires
            FROM {DB_SCHEMA}.dim_job dj
            LEFT JOIN {DB_SCHEMA}.fact_application fa ON dj.job_key = fa.job_key
            LEFT JOIN {DB_SCHEMA}.fact_hire fh ON dj.job_key = fh.job_key
            WHERE dj.is_current = TRUE
            GROUP BY dj.job_title
            ORDER BY applications DESC
            LIMIT 10
        """, use_spinner=False)
        
        if not job_analysis.empty:
            st.dataframe(job_analysis, use_container_width=True, hide_index=True)

# ==================== INTERVIEW ANALYTICS PAGE ====================
elif page == "Interview Analytics":
    st.header("🎤 Interview Analytics")
    
    with st.spinner("Loading interview data..."):
        interview_query = f"""
            SELECT 
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_interview) AS total,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_interview fi 
                 JOIN {DB_SCHEMA}.dim_interview_status dis ON fi.interview_status_key = dis.interview_status_key 
                 WHERE dis.is_current = TRUE AND dis.interview_status_name = 'Scheduled') AS scheduled,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_interview fi 
                 JOIN {DB_SCHEMA}.dim_interview_status dis ON fi.interview_status_key = dis.interview_status_key 
                 WHERE dis.is_current = TRUE AND dis.interview_status_name = 'Completed') AS completed
        """
        interview_stats = run_query(interview_query)
    
    col1, col2, col3, col4 = st.columns(4)
    
    if not interview_stats.empty:
        row = interview_stats.iloc[0]
        total = int(row['TOTAL'])
        completed = int(row['COMPLETED'])
        completion_rate = (completed / total * 100) if total > 0 else 0
        
        with col1:
            metric_card("Total Interviews", f"{total:,}")
        with col2:
            metric_card("Scheduled", f"{int(row['SCHEDULED']):,}")
        with col3:
            metric_card("Completed", f"{completed:,}")
        with col4:
            metric_card("Completion Rate", f"{completion_rate:.1f}%")
    
    st.divider()
    
    col_type, col_status = st.columns(2)
    
    with col_type:
        st.subheader("Interviews by Type")
        interview_type = safe_query(f"""
            SELECT 
                dit.interview_type_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_interview fi
            JOIN {DB_SCHEMA}.dim_interview_type dit ON fi.interview_type_key = dit.interview_type_key
            WHERE dit.is_current = TRUE
            GROUP BY dit.interview_type_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not interview_type.empty:
            plot_bar(interview_type, "INTERVIEW_TYPE_NAME", "CNT", "", color=COLORS[3], horizontal=True)
    
    with col_status:
        st.subheader("Interview Status")
        interview_status = safe_query(f"""
            SELECT 
                dis.interview_status_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_interview fi
            JOIN {DB_SCHEMA}.dim_interview_status dis ON fi.interview_status_key = dis.interview_status_key
            WHERE dis.is_current = TRUE
            GROUP BY dis.interview_status_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not interview_status.empty:
            plot_pie(interview_status, "INTERVIEW_STATUS_NAME", "CNT", "Distribution")

# ==================== RECRUITER PERFORMANCE PAGE ====================
elif page == "Recruiter Performance":
    st.header("👥 Recruiter Performance")
    
    st.subheader("Top Recruiters by Performance")
    with st.spinner("Loading recruiter data..."):
        recruiter_stats = safe_query(f"""
            SELECT 
                dr.recruiter_name,
                COUNT(DISTINCT fa.application_key) AS applications,
                COUNT(DISTINCT fh.hire_key) AS hires,
                ROUND(
                    CASE 
                        WHEN COUNT(DISTINCT fa.application_key) > 0 
                        THEN (COUNT(DISTINCT fh.hire_key)::NUMERIC / COUNT(DISTINCT fa.application_key) * 100)
                        ELSE 0
                    END, 2
                ) AS hire_rate
            FROM {DB_SCHEMA}.dim_recruiter dr
            LEFT JOIN {DB_SCHEMA}.fact_application fa ON dr.recruiter_key = fa.recruiter_key
            LEFT JOIN {DB_SCHEMA}.fact_hire fh ON dr.recruiter_key = fh.recruiter_key
            WHERE dr.is_current = TRUE
            GROUP BY dr.recruiter_name
            ORDER BY applications DESC
            LIMIT 15
        """, use_spinner=False)
    
    if not recruiter_stats.empty:
        st.dataframe(recruiter_stats, use_container_width=True, hide_index=True)
    
    st.divider()
    col_app, col_hire = st.columns(2)
    
    with col_app:
        st.subheader("Applications by Recruiter")
        recruiter_apps = safe_query(f"""
            SELECT 
                dr.recruiter_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_application fa
            JOIN {DB_SCHEMA}.dim_recruiter dr ON fa.recruiter_key = dr.recruiter_key
            WHERE dr.is_current = TRUE
            GROUP BY dr.recruiter_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not recruiter_apps.empty:
            plot_bar(recruiter_apps, "RECRUITER_NAME", "CNT", "", color=COLORS[0], horizontal=True)
    
    with col_hire:
        st.subheader("Hires by Recruiter")
        recruiter_hires = safe_query(f"""
            SELECT 
                dr.recruiter_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.fact_hire fh
            JOIN {DB_SCHEMA}.dim_recruiter dr ON fh.recruiter_key = dr.recruiter_key
            WHERE dr.is_current = TRUE
            GROUP BY dr.recruiter_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not recruiter_hires.empty:
            plot_bar(recruiter_hires, "RECRUITER_NAME", "CNT", "", color=SUCCESS_COLOR, horizontal=True)

# ==================== CANDIDATE ANALYTICS PAGE ====================
elif page == "Candidate Analytics":
    st.header("👨‍💼 Candidate Analytics")
    
    with st.spinner("Loading candidate data..."):
        candidate_query = f"""
            SELECT 
                (SELECT COUNT(*) FROM {DB_SCHEMA}.dim_candidate WHERE is_current = TRUE) AS total,
                (SELECT COUNT(DISTINCT candidate_key) FROM {DB_SCHEMA}.fact_hire) AS hired,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_application fa 
                 JOIN {DB_SCHEMA}.dim_application_status das ON fa.application_status_key = das.application_status_key 
                 WHERE das.is_current = TRUE AND das.application_status_name = 'Rejected') AS rejected
        """
        candidate_stats = run_query(candidate_query)
    
    col1, col2, col3, col4 = st.columns(4)
    
    if not candidate_stats.empty:
        row = candidate_stats.iloc[0]
        total = int(row['TOTAL'])
        hired = int(row['HIRED'])
        hire_rate = (hired / total * 100) if total > 0 else 0
        
        with col1:
            metric_card("Total Candidates", f"{total:,}")
        with col2:
            metric_card("Hired", f"{hired:,}")
        with col3:
            metric_card("Rejected", f"{int(row['REJECTED']):,}")
        with col4:
            metric_card("Hire Rate", f"{hire_rate:.1f}%")
    
    st.divider()
    
    col_gender, col_emp = st.columns(2)
    
    with col_gender:
        st.subheader("Candidates by Gender")
        gender_dist = safe_query(f"""
            SELECT 
                COALESCE(dg.gender_name, 'Unknown') AS gender_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.dim_candidate dc
            LEFT JOIN {DB_SCHEMA}.dim_gender dg ON dc.gender_key = dg.gender_key
            WHERE dc.is_current = TRUE
            GROUP BY dg.gender_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not gender_dist.empty:
            plot_pie(gender_dist, "GENDER_NAME", "CNT", "")
    
    with col_emp:
        st.subheader("Employment Type")
        emp_type = safe_query(f"""
            SELECT 
                COALESCE(det.employment_type_name, 'Unknown') AS employment_type_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.dim_candidate dc
            LEFT JOIN {DB_SCHEMA}.dim_employment_type det ON dc.employment_type_key = det.employment_type_key
            WHERE dc.is_current = TRUE
            GROUP BY det.employment_type_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not emp_type.empty:
            plot_pie(emp_type, "EMPLOYMENT_TYPE_NAME", "CNT", "")

# ==================== JOB ANALYTICS PAGE ====================
elif page == "Job Analytics":
    st.header("💼 Job Analytics")
    
    with st.spinner("Loading job data..."):
        job_query = f"""
            SELECT 
                (SELECT COUNT(*) FROM {DB_SCHEMA}.dim_job WHERE is_current = TRUE) AS total_jobs,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_application) AS total_applications,
                (SELECT COUNT(*) FROM {DB_SCHEMA}.fact_hire) AS total_hires
        """
        job_stats = run_query(job_query)
    
    col1, col2, col3, col4 = st.columns(4)
    
    if not job_stats.empty:
        row = job_stats.iloc[0]
        apps = int(row['TOTAL_APPLICATIONS'])
        hires = int(row['TOTAL_HIRES'])
        conversion = (hires / apps * 100) if apps > 0 else 0
        
        with col1:
            metric_card("Total Jobs", f"{int(row['TOTAL_JOBS']):,}")
        with col2:
            metric_card("Active Applications", f"{apps:,}")
        with col3:
            metric_card("Total Hires", f"{hires:,}")
        with col4:
            metric_card("Conversion", f"{conversion:.1f}%")
    
    st.divider()
    
    col_dept, col_work = st.columns(2)
    
    with col_dept:
        st.subheader("Jobs by Department")
        jobs_dept = safe_query(f"""
            SELECT 
                dd.department_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.dim_job dj
            JOIN {DB_SCHEMA}.dim_department dd ON dj.department_key = dd.department_key
            WHERE dj.is_current = TRUE AND dd.is_current = TRUE
            GROUP BY dd.department_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not jobs_dept.empty:
            plot_bar(jobs_dept, "DEPARTMENT_NAME", "CNT", "", color=COLORS[0], horizontal=True)
    
    with col_work:
        st.subheader("Jobs by Work Mode")
        jobs_work = safe_query(f"""
            SELECT 
                COALESCE(dwm.work_mode_name, 'Unknown') AS work_mode_name,
                COUNT(*) AS cnt
            FROM {DB_SCHEMA}.dim_job dj
            LEFT JOIN {DB_SCHEMA}.dim_work_mode dwm ON dj.work_mode_key = dwm.work_mode_key
            WHERE dj.is_current = TRUE
            GROUP BY dwm.work_mode_name
            ORDER BY cnt DESC
            LIMIT 10
        """, use_spinner=False)
        if not jobs_work.empty:
            plot_pie(jobs_work, "WORK_MODE_NAME", "CNT", "")
    
    st.divider()
    st.subheader("💼 Job Details & Performance")
    with st.spinner("Loading job details..."):
        job_details = safe_query(f"""
            SELECT 
                dj.job_title,
                COALESCE(dd.designation_name, 'N/A') AS designation,
                COALESCE(dept.department_name, 'N/A') AS department,
                COUNT(DISTINCT fa.application_key) AS applications,
                COUNT(DISTINCT fh.hire_key) AS hires
            FROM {DB_SCHEMA}.dim_job dj
            LEFT JOIN {DB_SCHEMA}.dim_designation dd ON dj.designation_key = dd.designation_key AND dd.is_current = TRUE
            LEFT JOIN {DB_SCHEMA}.dim_department dept ON dj.department_key = dept.department_key AND dept.is_current = TRUE
            LEFT JOIN {DB_SCHEMA}.fact_application fa ON dj.job_key = fa.job_key
            LEFT JOIN {DB_SCHEMA}.fact_hire fh ON dj.job_key = fh.job_key
            WHERE dj.is_current = TRUE
            GROUP BY dj.job_title, dd.designation_name, dept.department_name
            ORDER BY applications DESC
            LIMIT 15
        """, use_spinner=False)
    
    if not job_details.empty:
        st.dataframe(job_details, use_container_width=True, hide_index=True)

# Footer
st.divider()
st.markdown("---")
st.markdown(f"*Dashboard Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
