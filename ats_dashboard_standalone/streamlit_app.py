import streamlit as st
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt

st.set_page_config(page_title="ATS Recruitment Dashboard", layout="wide")

DB_SCHEMA = "ATS_DWH.DWH"

COLORS = ["#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F",
          "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC"]

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        role=st.secrets["snowflake"].get("role", "ACCOUNTADMIN"),
        client_session_keep_alive=True,
    )

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

@st.cache_data(ttl=300)
def run_query(sql):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    cur.close()
    return pd.DataFrame(data, columns=columns)

def safe_query(sql):
    try:
        df = run_query(sql)
        if df.empty:
            return pd.DataFrame()
        return df
    except Exception as e:
        st.warning(f"Query error: {e}")
        return pd.DataFrame()

def metric_card(label, value, delta=None):
    st.metric(label=label, value=value, delta=delta)

def plot_bar(data, x_col, y_col, title, color=COLORS[0], horizontal=False, figsize=(8, 4)):
    data = data.copy()
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce").fillna(0)
    labels = data[x_col].astype(str).tolist()
    values = [float(v) for v in data[y_col]]
    fig, ax = plt.subplots(figsize=figsize)
    if horizontal:
        y_pos = list(range(len(labels)))
        bars = ax.barh(y_pos, values, color=color, edgecolor="none", height=0.6)
        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        ax.invert_yaxis()
        max_val = max(values) if values else 1
        for bar, val in zip(bars, values):
            ax.text(bar.get_width() + max_val * 0.02, bar.get_y() + bar.get_height() / 2,
                    f"{val:,.0f}", va="center", ha="left", fontsize=9)
        style_ax(ax, title, xlabel=y_col.replace("_", " ").title())
    else:
        bars = ax.bar(range(len(labels)), values, color=color, edgecolor="none", width=0.6)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=8)
        max_val = max(values) if values else 1
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max_val * 0.02,
                    f"{val:,.0f}", ha="center", va="bottom", fontsize=8)
        style_ax(ax, title)
    fig.tight_layout()
    st.pyplot(fig, transparent=True)
    plt.close(fig)

def plot_pie(data, label_col, value_col, title, figsize=(6, 4)):
    data = data.copy()
    data[value_col] = pd.to_numeric(data[value_col], errors="coerce").fillna(0)
    fig, ax = plt.subplots(figsize=figsize)
    fig.patch.set_alpha(0.0)
    ax.set_facecolor("none")
    wedges, texts, autotexts = ax.pie(
        data[value_col].tolist(), labels=data[label_col].astype(str).tolist(),
        autopct="%1.1f%%", colors=COLORS[:len(data)], startangle=140, pctdistance=0.8,
        textprops={"fontsize": 9}
    )
    for t in autotexts:
        t.set_fontsize(8)
    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    fig.tight_layout()
    st.pyplot(fig, transparent=True)
    plt.close(fig)

def plot_line(data, x_col, y_col, title, color=COLORS[0], figsize=(10, 4)):
    data = data.copy()
    data[y_col] = pd.to_numeric(data[y_col], errors="coerce").fillna(0)
    labels = data[x_col].astype(str).tolist()
    values = [float(v) for v in data[y_col]]
    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(labels, values, color=color, marker="o", linewidth=2, markersize=5)
    ax.fill_between(labels, values, alpha=0.15, color=color)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=8)
    style_ax(ax, title)
    fig.tight_layout()
    st.pyplot(fig, transparent=True)
    plt.close(fig)

st.title("ATS Recruitment Dashboard")

page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Hiring Pipeline", "Interview Analytics", "Hiring Efficiency", "Recruiter Performance", "Job & Skill Analytics"]
)

if page == "Overview":
    st.header("Overview")

    col1, col2, col3, col4 = st.columns(4)

    total_apps = safe_query(f"SELECT COUNT(*) AS CNT FROM {DB_SCHEMA}.FACT_APPLICATION")
    total_interviews = safe_query(f"SELECT COUNT(*) AS CNT FROM {DB_SCHEMA}.FACT_INTERVIEW")
    total_hires = safe_query(f"SELECT COUNT(*) AS CNT FROM {DB_SCHEMA}.FACT_HIRE")
    total_candidates = safe_query(f"SELECT COUNT(*) AS CNT FROM {DB_SCHEMA}.DIM_CANDIDATE WHERE IS_CURRENT = TRUE")

    with col1:
        metric_card("Total Applications", total_apps["CNT"].iloc[0] if not total_apps.empty else 0)
    with col2:
        metric_card("Total Interviews", total_interviews["CNT"].iloc[0] if not total_interviews.empty else 0)
    with col3:
        metric_card("Total Hires", total_hires["CNT"].iloc[0] if not total_hires.empty else 0)
    with col4:
        metric_card("Active Candidates", total_candidates["CNT"].iloc[0] if not total_candidates.empty else 0)

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        apps_time = safe_query(f"""
            SELECT d.YEAR_NUMBER, d.MONTH_NUMBER, d.MONTH_NAME,
                   COUNT(*) AS APPLICATION_COUNT
            FROM {DB_SCHEMA}.FACT_APPLICATION fa
            JOIN {DB_SCHEMA}.DIM_DATE d ON fa.APPLICATION_DATE_KEY = d.DATE_KEY
            GROUP BY d.YEAR_NUMBER, d.MONTH_NUMBER, d.MONTH_NAME
            ORDER BY d.YEAR_NUMBER, d.MONTH_NUMBER
        """)
        if not apps_time.empty:
            apps_time["PERIOD"] = apps_time["MONTH_NAME"].str[:3] + " " + apps_time["YEAR_NUMBER"].astype(str)
            plot_line(apps_time, "PERIOD", "APPLICATION_COUNT", "Applications Over Time", color=COLORS[0], figsize=(6, 4))
        else:
            st.info("No application data available yet.")

    with col_right:
        hires_time = safe_query(f"""
            SELECT d.YEAR_NUMBER, d.MONTH_NUMBER, d.MONTH_NAME,
                   COUNT(*) AS HIRE_COUNT
            FROM {DB_SCHEMA}.FACT_HIRE fh
            JOIN {DB_SCHEMA}.DIM_DATE d ON fh.HIRE_DATE_KEY = d.DATE_KEY
            GROUP BY d.YEAR_NUMBER, d.MONTH_NUMBER, d.MONTH_NAME
            ORDER BY d.YEAR_NUMBER, d.MONTH_NUMBER
        """)
        if not hires_time.empty:
            hires_time["PERIOD"] = hires_time["MONTH_NAME"].str[:3] + " " + hires_time["YEAR_NUMBER"].astype(str)
            plot_line(hires_time, "PERIOD", "HIRE_COUNT", "Hires Over Time", color=COLORS[4], figsize=(6, 4))
        else:
            st.info("No hire data available yet.")

    st.divider()
    status_dist = safe_query(f"""
        SELECT das.APPLICATION_STATUS_NAME AS STATUS, COUNT(*) AS CNT
        FROM {DB_SCHEMA}.FACT_APPLICATION fa
        JOIN {DB_SCHEMA}.DIM_APPLICATION_STATUS das
            ON fa.APPLICATION_STATUS_KEY = das.APPLICATION_STATUS_KEY
        GROUP BY das.APPLICATION_STATUS_NAME
        ORDER BY CNT DESC
    """)
    if not status_dist.empty:
        col_pie, col_bar = st.columns(2)
        with col_pie:
            plot_pie(status_dist, "STATUS", "CNT", "Application Status Distribution", figsize=(6, 4))
        with col_bar:
            plot_bar(status_dist, "STATUS", "CNT", "Application Status Counts", color=COLORS[2], figsize=(6, 4))
    else:
        st.info("No application status data available yet.")


elif page == "Hiring Pipeline":
    st.header("Hiring Pipeline")

    col1, col2, col3, col4, col5 = st.columns(5)
    pipeline_kpis = safe_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(IS_SELECTED) AS SELECTED,
            SUM(IS_REJECTED) AS REJECTED,
            SUM(IS_ON_HOLD) AS ON_HOLD,
            SUM(IS_COMPLETED) AS COMPLETED
        FROM {DB_SCHEMA}.FACT_APPLICATION
    """)
    if not pipeline_kpis.empty:
        row = pipeline_kpis.iloc[0]
        total = int(row["TOTAL"]) if pd.notna(row["TOTAL"]) else 0
        with col1:
            metric_card("Total Applications", total)
        with col2:
            sel = int(row["SELECTED"]) if pd.notna(row["SELECTED"]) else 0
            metric_card("Selected", sel, f"{sel/total*100:.1f}%" if total > 0 else None)
        with col3:
            rej = int(row["REJECTED"]) if pd.notna(row["REJECTED"]) else 0
            metric_card("Rejected", rej, f"{rej/total*100:.1f}%" if total > 0 else None)
        with col4:
            oh = int(row["ON_HOLD"]) if pd.notna(row["ON_HOLD"]) else 0
            metric_card("On Hold", oh, f"{oh/total*100:.1f}%" if total > 0 else None)
        with col5:
            comp = int(row["COMPLETED"]) if pd.notna(row["COMPLETED"]) else 0
            metric_card("Completed", comp, f"{comp/total*100:.1f}%" if total > 0 else None)

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        by_source = safe_query(f"""
            SELECT ps.PROFILE_SOURCE_NAME AS SOURCE, COUNT(*) AS CNT
            FROM {DB_SCHEMA}.FACT_APPLICATION fa
            JOIN {DB_SCHEMA}.DIM_PROFILE_SOURCE ps ON fa.PROFILE_SOURCE_KEY = ps.PROFILE_SOURCE_KEY
            GROUP BY ps.PROFILE_SOURCE_NAME
            ORDER BY CNT DESC
        """)
        if not by_source.empty:
            plot_bar(by_source, "SOURCE", "CNT", "Applications by Source", color=COLORS[1], horizontal=True, figsize=(6, 4))
        else:
            st.info("No source data available yet.")

    with col_right:
        by_dept = safe_query(f"""
            SELECT dj.DEPARTMENT_NAME AS DEPARTMENT, COUNT(*) AS CNT
            FROM {DB_SCHEMA}.FACT_APPLICATION fa
            JOIN {DB_SCHEMA}.DIM_JOB dj ON fa.JOB_KEY = dj.JOB_KEY AND dj.IS_CURRENT = TRUE
            GROUP BY dj.DEPARTMENT_NAME
            ORDER BY CNT DESC
        """)
        if not by_dept.empty:
            plot_bar(by_dept, "DEPARTMENT", "CNT", "Applications by Department", color=COLORS[0], horizontal=True, figsize=(6, 4))
        else:
            st.info("No department data available yet.")

    st.divider()
    st.subheader("CTC Analysis (Applications)")
    ctc_analysis = safe_query(f"""
        SELECT
            ROUND(AVG(CANDIDATE_CURRENT_CTC), 2) AS AVG_CURRENT_CTC,
            ROUND(AVG(CANDIDATE_EXPECTED_CTC), 2) AS AVG_EXPECTED_CTC,
            ROUND(AVG(CONFIRMED_CTC), 2) AS AVG_CONFIRMED_CTC,
            ROUND(AVG(CTC_VARIANCE), 2) AS AVG_CTC_VARIANCE
        FROM {DB_SCHEMA}.FACT_APPLICATION
        WHERE CANDIDATE_CURRENT_CTC IS NOT NULL
    """)
    if not ctc_analysis.empty and pd.notna(ctc_analysis.iloc[0]["AVG_CURRENT_CTC"]):
        c1, c2, c3, c4 = st.columns(4)
        r = ctc_analysis.iloc[0]
        with c1:
            metric_card("Avg Current CTC", f"₹{r['AVG_CURRENT_CTC']:,.0f}" if pd.notna(r["AVG_CURRENT_CTC"]) else "N/A")
        with c2:
            metric_card("Avg Expected CTC", f"₹{r['AVG_EXPECTED_CTC']:,.0f}" if pd.notna(r["AVG_EXPECTED_CTC"]) else "N/A")
        with c3:
            metric_card("Avg Confirmed CTC", f"₹{r['AVG_CONFIRMED_CTC']:,.0f}" if pd.notna(r["AVG_CONFIRMED_CTC"]) else "N/A")
        with c4:
            metric_card("Avg CTC Variance", f"₹{r['AVG_CTC_VARIANCE']:,.0f}" if pd.notna(r["AVG_CTC_VARIANCE"]) else "N/A")
    else:
        st.info("No CTC data available yet.")


elif page == "Interview Analytics":
    st.header("Interview Analytics")

    col1, col2, col3, col4 = st.columns(4)
    int_kpis = safe_query(f"""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(IS_COMPLETED) AS COMPLETED,
            SUM(IS_CANCELLED) AS CANCELLED,
            SUM(IS_SCHEDULED) AS SCHEDULED
        FROM {DB_SCHEMA}.FACT_INTERVIEW
    """)
    if not int_kpis.empty:
        row = int_kpis.iloc[0]
        total = int(row["TOTAL"]) if pd.notna(row["TOTAL"]) else 0
        with col1:
            metric_card("Total Interviews", total)
        with col2:
            comp = int(row["COMPLETED"]) if pd.notna(row["COMPLETED"]) else 0
            metric_card("Completed", comp, f"{comp/total*100:.1f}%" if total > 0 else None)
        with col3:
            canc = int(row["CANCELLED"]) if pd.notna(row["CANCELLED"]) else 0
            metric_card("Cancelled", canc, f"{canc/total*100:.1f}%" if total > 0 else None)
        with col4:
            sched = int(row["SCHEDULED"]) if pd.notna(row["SCHEDULED"]) else 0
            metric_card("Scheduled", sched, f"{sched/total*100:.1f}%" if total > 0 else None)

    st.divider()

    st.subheader("Average Feedback Ratings")
    ratings = safe_query(f"""
        SELECT
            ROUND(AVG(COMMUNICATION_RATING_AVG), 2) AS AVG_COMMUNICATION,
            ROUND(AVG(TECHNICAL_RATING_AVG), 2) AS AVG_TECHNICAL,
            ROUND(AVG(LOGICAL_RATING_AVG), 2) AS AVG_LOGICAL,
            ROUND(AVG(OVERALL_FEEDBACK_AVG), 2) AS AVG_OVERALL
        FROM {DB_SCHEMA}.FACT_INTERVIEW
        WHERE FEEDBACK_COUNT > 0
    """)
    if not ratings.empty and pd.notna(ratings.iloc[0]["AVG_COMMUNICATION"]):
        rating_data = pd.DataFrame({
            "CATEGORY": ["Communication", "Technical", "Logical", "Overall"],
            "RATING": [
                float(ratings.iloc[0]["AVG_COMMUNICATION"]),
                float(ratings.iloc[0]["AVG_TECHNICAL"]),
                float(ratings.iloc[0]["AVG_LOGICAL"]),
                float(ratings.iloc[0]["AVG_OVERALL"])
            ]
        })
        plot_bar(rating_data, "CATEGORY", "RATING", "Average Feedback Ratings",
                 color=COLORS[3], figsize=(8, 4))
    else:
        st.info("No feedback rating data available yet.")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        by_type = safe_query(f"""
            SELECT dit.INTERVIEW_TYPE_NAME AS TYPE, COUNT(*) AS CNT
            FROM {DB_SCHEMA}.FACT_INTERVIEW fi
            JOIN {DB_SCHEMA}.DIM_INTERVIEW_TYPE dit ON fi.INTERVIEW_TYPE_KEY = dit.INTERVIEW_TYPE_KEY
            GROUP BY dit.INTERVIEW_TYPE_NAME ORDER BY CNT DESC
        """)
        if not by_type.empty:
            plot_pie(by_type, "TYPE", "CNT", "Interviews by Type", figsize=(6, 4))
        else:
            st.info("No interview type data available yet.")

    with col_right:
        by_stage = safe_query(f"""
            SELECT das.APPLICATION_STAGE_NAME AS STAGE, COUNT(*) AS CNT
            FROM {DB_SCHEMA}.FACT_INTERVIEW fi
            JOIN {DB_SCHEMA}.DIM_APPLICATION_STAGE das ON fi.APPLICATION_STAGE_KEY = das.APPLICATION_STAGE_KEY
            GROUP BY das.APPLICATION_STAGE_NAME ORDER BY CNT DESC
        """)
        if not by_stage.empty:
            plot_bar(by_stage, "STAGE", "CNT", "Interviews by Stage", color=COLORS[5], figsize=(6, 4))
        else:
            st.info("No interview stage data available yet.")

    st.divider()
    avg_dur = safe_query(f"""
        SELECT
            das.APPLICATION_STAGE_NAME AS STAGE,
            ROUND(AVG(fi.INTERVIEW_DURATION_MINUTES), 1) AS AVG_DURATION
        FROM {DB_SCHEMA}.FACT_INTERVIEW fi
        JOIN {DB_SCHEMA}.DIM_APPLICATION_STAGE das ON fi.APPLICATION_STAGE_KEY = das.APPLICATION_STAGE_KEY
        GROUP BY das.APPLICATION_STAGE_NAME ORDER BY AVG_DURATION DESC
    """)
    if not avg_dur.empty:
        plot_bar(avg_dur, "STAGE", "AVG_DURATION", "Avg Interview Duration (minutes)",
                 color=COLORS[6], horizontal=True, figsize=(10, 4))
    else:
        st.info("No duration data available yet.")


elif page == "Hiring Efficiency":
    st.header("Hiring Efficiency")

    col1, col2, col3 = st.columns(3)
    eff_kpis = safe_query(f"""
        SELECT
            ROUND(AVG(TIME_TO_HIRE_DAYS), 1) AS AVG_TIME_TO_HIRE,
            MIN(TIME_TO_HIRE_DAYS) AS MIN_TTH,
            MAX(TIME_TO_HIRE_DAYS) AS MAX_TTH
        FROM {DB_SCHEMA}.FACT_HIRE
    """)
    if not eff_kpis.empty and pd.notna(eff_kpis.iloc[0]["AVG_TIME_TO_HIRE"]):
        with col1:
            metric_card("Avg Time to Hire (days)", eff_kpis.iloc[0]["AVG_TIME_TO_HIRE"])
        with col2:
            v = eff_kpis.iloc[0]["MIN_TTH"]
            metric_card("Min Time to Hire", int(v) if pd.notna(v) else 0)
        with col3:
            v = eff_kpis.iloc[0]["MAX_TTH"]
            metric_card("Max Time to Hire", int(v) if pd.notna(v) else 0)
    else:
        with col1:
            st.info("No hire data available yet.")

    hire_rate = safe_query(f"""
        SELECT
            (SELECT COUNT(*) FROM {DB_SCHEMA}.FACT_HIRE) AS HIRES,
            (SELECT COUNT(*) FROM {DB_SCHEMA}.FACT_APPLICATION) AS APPS
    """)
    if not hire_rate.empty:
        hires = int(hire_rate.iloc[0]["HIRES"]) if pd.notna(hire_rate.iloc[0]["HIRES"]) else 0
        apps = int(hire_rate.iloc[0]["APPS"]) if pd.notna(hire_rate.iloc[0]["APPS"]) else 0
        rate = f"{hires/apps*100:.1f}%" if apps > 0 else "N/A"
        st.metric("Overall Hire Rate", rate, f"{hires} hires / {apps} applications")

    st.divider()

    st.subheader("CTC Comparison (Hires)")
    ctc_hire = safe_query(f"""
        SELECT
            ROUND(AVG(CURRENT_CTC), 2) AS AVG_CURRENT,
            ROUND(AVG(EXPECTED_CTC), 2) AS AVG_EXPECTED,
            ROUND(AVG(CONFIRMED_CTC), 2) AS AVG_CONFIRMED,
            ROUND(AVG(CTC_CHANGE_FROM_CURRENT), 2) AS AVG_CTC_CHANGE
        FROM {DB_SCHEMA}.FACT_HIRE
        WHERE CURRENT_CTC IS NOT NULL
    """)
    if not ctc_hire.empty and pd.notna(ctc_hire.iloc[0]["AVG_CURRENT"]):
        r = ctc_hire.iloc[0]
        ctc_data = pd.DataFrame({
            "CATEGORY": ["Current CTC", "Expected CTC", "Confirmed CTC"],
            "AMOUNT": [
                float(r["AVG_CURRENT"]) if pd.notna(r["AVG_CURRENT"]) else 0,
                float(r["AVG_EXPECTED"]) if pd.notna(r["AVG_EXPECTED"]) else 0,
                float(r["AVG_CONFIRMED"]) if pd.notna(r["AVG_CONFIRMED"]) else 0
            ]
        })
        plot_bar(ctc_data, "CATEGORY", "AMOUNT", "Avg CTC Comparison (Hires)",
                 color=COLORS[4], figsize=(8, 4))
        v = r["AVG_CTC_CHANGE"]
        st.metric("Avg CTC Change from Current",
                  f"₹{v:,.0f}" if pd.notna(v) else "N/A")
    else:
        st.info("No CTC hire data available yet.")

    st.divider()
    tth_dept = safe_query(f"""
        SELECT dj.DEPARTMENT_NAME AS DEPARTMENT,
               ROUND(AVG(fh.TIME_TO_HIRE_DAYS), 1) AS AVG_TTH,
               COUNT(*) AS HIRES
        FROM {DB_SCHEMA}.FACT_HIRE fh
        JOIN {DB_SCHEMA}.FACT_APPLICATION fa ON fh.APPLICATION_KEY = fa.FACT_APPLICATION_KEY
        JOIN {DB_SCHEMA}.DIM_JOB dj ON fa.JOB_KEY = dj.JOB_KEY AND dj.IS_CURRENT = TRUE
        GROUP BY dj.DEPARTMENT_NAME ORDER BY AVG_TTH DESC
    """)
    if not tth_dept.empty:
        plot_bar(tth_dept, "DEPARTMENT", "AVG_TTH", "Time to Hire by Department (days)",
                 color=COLORS[2], horizontal=True, figsize=(10, 5))
        st.dataframe(tth_dept, use_container_width=True)
    else:
        st.info("No department hire data available yet.")

    st.divider()
    tth_dist = safe_query(f"""
        SELECT
            CASE
                WHEN TIME_TO_HIRE_DAYS <= 7 THEN '0-7 days'
                WHEN TIME_TO_HIRE_DAYS <= 15 THEN '8-15 days'
                WHEN TIME_TO_HIRE_DAYS <= 30 THEN '16-30 days'
                WHEN TIME_TO_HIRE_DAYS <= 60 THEN '31-60 days'
                ELSE '60+ days'
            END AS BUCKET,
            COUNT(*) AS CNT
        FROM {DB_SCHEMA}.FACT_HIRE
        GROUP BY BUCKET ORDER BY BUCKET
    """)
    if not tth_dist.empty:
        plot_bar(tth_dist, "BUCKET", "CNT", "Time to Hire Distribution", color=COLORS[5])
    else:
        st.info("No hire data available yet.")


elif page == "Recruiter Performance":
    st.header("Recruiter Performance")

    top_rec_apps = safe_query(f"""
        SELECT dr.RECRUITER_NAME, COUNT(*) AS APPLICATIONS
        FROM {DB_SCHEMA}.FACT_APPLICATION fa
        JOIN {DB_SCHEMA}.DIM_RECRUITER dr ON fa.RECRUITER_KEY = dr.RECRUITER_KEY AND dr.IS_CURRENT = TRUE
        GROUP BY dr.RECRUITER_NAME ORDER BY APPLICATIONS DESC
        LIMIT 15
    """)
    if not top_rec_apps.empty:
        plot_bar(top_rec_apps, "RECRUITER_NAME", "APPLICATIONS",
                 "Top Recruiters by Applications", color=COLORS[0], horizontal=True, figsize=(10, 6))
    else:
        st.info("No recruiter application data available yet.")

    st.divider()

    top_rec_hires = safe_query(f"""
        SELECT dr.RECRUITER_NAME,
               COUNT(*) AS HIRES,
               ROUND(AVG(fh.TIME_TO_HIRE_DAYS), 1) AS AVG_TTH
        FROM {DB_SCHEMA}.FACT_HIRE fh
        JOIN {DB_SCHEMA}.DIM_RECRUITER dr ON fh.RECRUITER_KEY = dr.RECRUITER_KEY AND dr.IS_CURRENT = TRUE
        GROUP BY dr.RECRUITER_NAME ORDER BY HIRES DESC
        LIMIT 15
    """)
    if not top_rec_hires.empty:
        plot_bar(top_rec_hires, "RECRUITER_NAME", "HIRES",
                 "Top Recruiters by Hires", color=COLORS[4], horizontal=True, figsize=(10, 6))
        st.dataframe(top_rec_hires, use_container_width=True)
    else:
        st.info("No recruiter hire data available yet.")

    st.divider()

    st.subheader("Recruiter Conversion Rate (Hires / Applications)")
    rec_conv = safe_query(f"""
        SELECT
            dr.RECRUITER_NAME,
            COUNT(DISTINCT fa.APPLICATION_ID) AS APPLICATIONS,
            COUNT(DISTINCT fh.APPLICATION_NUMBER) AS HIRES,
            ROUND(COUNT(DISTINCT fh.APPLICATION_NUMBER) * 100.0 / NULLIF(COUNT(DISTINCT fa.APPLICATION_ID), 0), 1) AS CONVERSION_PCT
        FROM {DB_SCHEMA}.DIM_RECRUITER dr
        JOIN {DB_SCHEMA}.FACT_APPLICATION fa ON fa.RECRUITER_KEY = dr.RECRUITER_KEY
        LEFT JOIN {DB_SCHEMA}.FACT_HIRE fh ON fh.RECRUITER_KEY = dr.RECRUITER_KEY
        WHERE dr.IS_CURRENT = TRUE
        GROUP BY dr.RECRUITER_NAME
        HAVING COUNT(DISTINCT fa.APPLICATION_ID) >= 1
        ORDER BY CONVERSION_PCT DESC
        LIMIT 15
    """)
    if not rec_conv.empty:
        plot_bar(rec_conv, "RECRUITER_NAME", "CONVERSION_PCT",
                 "Recruiter Conversion Rate (%)", color=COLORS[1], horizontal=True, figsize=(10, 6))
        st.dataframe(rec_conv, use_container_width=True)
    else:
        st.info("No recruiter conversion data available yet.")


elif page == "Job & Skill Analytics":
    st.header("Job & Skill Analytics")

    col1, col2 = st.columns(2)

    with col1:
        jobs_wm = safe_query(f"""
            SELECT WORK_MODE_NAME AS WORK_MODE, COUNT(*) AS JOBS
            FROM {DB_SCHEMA}.DIM_JOB
            WHERE IS_CURRENT = TRUE AND IS_ACTIVE = TRUE
            GROUP BY WORK_MODE_NAME ORDER BY JOBS DESC
        """)
        if not jobs_wm.empty:
            plot_pie(jobs_wm, "WORK_MODE", "JOBS", "Jobs by Work Mode", figsize=(6, 4))
        else:
            st.info("No work mode data available yet.")

    with col2:
        jobs_et = safe_query(f"""
            SELECT EMPLOYMENT_TYPE_NAME AS TYPE, COUNT(*) AS JOBS
            FROM {DB_SCHEMA}.DIM_JOB
            WHERE IS_CURRENT = TRUE AND IS_ACTIVE = TRUE
            GROUP BY EMPLOYMENT_TYPE_NAME ORDER BY JOBS DESC
        """)
        if not jobs_et.empty:
            plot_pie(jobs_et, "TYPE", "JOBS", "Jobs by Employment Type", figsize=(6, 4))
        else:
            st.info("No employment type data available yet.")

    st.divider()

    jobs_client = safe_query(f"""
        SELECT CLIENT_NAME AS CLIENT, COUNT(*) AS JOBS
        FROM {DB_SCHEMA}.DIM_JOB
        WHERE IS_CURRENT = TRUE AND IS_ACTIVE = TRUE AND CLIENT_NAME IS NOT NULL
        GROUP BY CLIENT_NAME ORDER BY JOBS DESC
        LIMIT 10
    """)
    if not jobs_client.empty:
        plot_bar(jobs_client, "CLIENT", "JOBS", "Jobs by Client",
                 color=COLORS[3], horizontal=True, figsize=(10, 5))
    else:
        st.info("No client job data available yet.")

    st.divider()

    top_skills = safe_query(f"""
        SELECT ds.SKILL_NAME, COUNT(*) AS DEMAND_COUNT
        FROM {DB_SCHEMA}.BRIDGE_JOB_SKILL bjs
        JOIN {DB_SCHEMA}.DIM_SKILL ds ON bjs.SKILL_KEY = ds.SKILL_KEY AND ds.IS_CURRENT = TRUE
        GROUP BY ds.SKILL_NAME ORDER BY DEMAND_COUNT DESC
        LIMIT 20
    """)
    if not top_skills.empty:
        plot_bar(top_skills, "SKILL_NAME", "DEMAND_COUNT",
                 "Top Skills in Demand (Job Requirements)", color=COLORS[0], horizontal=True, figsize=(10, 7))
    else:
        st.info("No skill demand data available yet.")

    st.divider()

    cand_skills = safe_query(f"""
        SELECT ds.SKILL_NAME, COUNT(*) AS CANDIDATE_COUNT,
               ROUND(AVG(bcs.RELEVANT_EXPERIENCE), 1) AS AVG_EXPERIENCE,
               ROUND(AVG(bcs.SELF_RATING), 1) AS AVG_SELF_RATING
        FROM {DB_SCHEMA}.BRIDGE_CANDIDATE_SKILL bcs
        JOIN {DB_SCHEMA}.DIM_SKILL ds ON bcs.SKILL_KEY = ds.SKILL_KEY AND ds.IS_CURRENT = TRUE
        GROUP BY ds.SKILL_NAME ORDER BY CANDIDATE_COUNT DESC
        LIMIT 20
    """)
    if not cand_skills.empty:
        plot_bar(cand_skills, "SKILL_NAME", "CANDIDATE_COUNT",
                 "Top Candidate Skills", color=COLORS[6], horizontal=True, figsize=(10, 7))
        st.dataframe(cand_skills, use_container_width=True)
    else:
        st.info("No candidate skill data available yet.")

    st.divider()
    st.subheader("Vacancy Summary")
    vacancy = safe_query(f"""
        SELECT
            SUM(VACANCY_COUNT) AS TOTAL_VACANCIES,
            COUNT(*) AS TOTAL_ACTIVE_JOBS,
            ROUND(AVG(VACANCY_COUNT), 1) AS AVG_VACANCIES_PER_JOB
        FROM {DB_SCHEMA}.DIM_JOB
        WHERE IS_CURRENT = TRUE AND IS_ACTIVE = TRUE
    """)
    if not vacancy.empty and pd.notna(vacancy.iloc[0]["TOTAL_ACTIVE_JOBS"]):
        c1, c2, c3 = st.columns(3)
        with c1:
            v = vacancy.iloc[0]["TOTAL_VACANCIES"]
            metric_card("Total Vacancies", int(v) if pd.notna(v) else 0)
        with c2:
            metric_card("Active Jobs", int(vacancy.iloc[0]["TOTAL_ACTIVE_JOBS"]))
        with c3:
            v = vacancy.iloc[0]["AVG_VACANCIES_PER_JOB"]
            metric_card("Avg Vacancies/Job", v if pd.notna(v) else 0)
    else:
        st.info("No vacancy data available yet.")
