-- =====================================================================
-- ats_dwh CLEAN CREATE SCRIPT (POSTGRESQL)
-- Derived from: ats_dwh_updated_schema_24042026.sql
-- Purpose: clean schema-only DDL for PostgreSQL deployment
-- SCD2-ready dimension model
-- Audit timestamp convention:
--   target created_date / updated_date are warehouse-managed by ETL
--   for dimensions:
--     created_date = CURRENT_TIMESTAMP on insert
--     updated_date = NULL for active/current row
--     updated_date = CURRENT_TIMESTAMP when that row is expired
--   for facts and bridges:
--     created_date / updated_date = CURRENT_TIMESTAMP on load
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ats_dwh;


CREATE TABLE IF NOT EXISTS ats_dwh.dim_country (
    country_key BIGSERIAL PRIMARY KEY,
    country_id BIGINT NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_states (
    states_key BIGSERIAL PRIMARY KEY,
    states_id BIGINT NOT NULL,
    country_id BIGINT NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT,
    CONSTRAINT fk_states_country
        FOREIGN KEY (country_id) REFERENCES ats_dwh.dim_country(country_key)
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_month SMALLINT NOT NULL,
    month_number SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter_number SMALLINT NOT NULL,
    year_number INTEGER NOT NULL,
    week_of_year SMALLINT NOT NULL,
    day_of_week_number SMALLINT NOT NULL,
    day_of_week_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_department (
    department_key BIGSERIAL PRIMARY KEY,
    department_id BIGINT NOT NULL,
    department_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_designation (
    designation_key BIGSERIAL PRIMARY KEY,
    designation_id BIGINT NOT NULL,
    designation_name VARCHAR(150) NOT NULL,
    is_active BOOLEAN,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_employment_type (
    employment_type_key BIGSERIAL PRIMARY KEY,
    employment_type_id BIGINT NOT NULL,
    employment_type_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_work_mode (
    work_mode_key BIGSERIAL PRIMARY KEY,
    work_mode_id BIGINT NOT NULL,
    work_mode_name VARCHAR(50) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_gender (
    gender_key BIGSERIAL PRIMARY KEY,
    gender_id BIGINT NOT NULL,
    gender_name VARCHAR(50) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_profile_source (
    profile_source_key BIGSERIAL PRIMARY KEY,
    profile_source_id BIGINT NOT NULL,
    profile_source_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_profile_status (
    profile_status_key BIGSERIAL PRIMARY KEY,
    profile_status_id BIGINT NOT NULL,
    profile_status_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_application_status (
    application_status_key BIGSERIAL PRIMARY KEY,
    application_status_id BIGINT NOT NULL,
    application_status_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_application_stage (
    application_stage_key BIGSERIAL PRIMARY KEY,
    application_stage_id BIGINT NOT NULL,
    application_stage_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_interview_status (
    interview_status_key BIGSERIAL PRIMARY KEY,
    interview_status_id BIGINT NOT NULL,
    interview_status_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_interview_type (
    interview_type_key BIGSERIAL PRIMARY KEY,
    interview_type_id BIGINT NOT NULL,
    interview_type_name VARCHAR(100) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_client (
    client_key BIGSERIAL PRIMARY KEY,
    client_id BIGINT NOT NULL,
    client_name VARCHAR(150) NOT NULL,
    client_location VARCHAR(150),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_skill (
    skill_key BIGSERIAL PRIMARY KEY,
    skill_master_id BIGINT NOT NULL,
    skill_name VARCHAR(150) NOT NULL,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_recruiter (
    recruiter_key BIGSERIAL PRIMARY KEY,
    employee_id BIGINT NOT NULL,
    employee_number VARCHAR(50) NOT NULL,
    recruiter_name VARCHAR(201) NOT NULL,
    role_id BIGINT,
    role_name VARCHAR(100),
    designation_id BIGINT,
    designation_name VARCHAR(150),
    gender_id BIGINT,
    gender_name VARCHAR(50),
    mail_id VARCHAR(255),
    phone_number VARCHAR(20),
    experience_years NUMERIC(5,2),
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_candidate (
    candidate_key BIGSERIAL PRIMARY KEY,
    candidate_id BIGINT NOT NULL,
    candidate_number VARCHAR(50) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100),
    full_name VARCHAR(201) NOT NULL,
    gender_id BIGINT,
    gender_name VARCHAR(50),
    email_id VARCHAR(255),
    contact_number VARCHAR(20),
    date_of_birth DATE,
    current_ctc NUMERIC(12,2),
    expected_ctc NUMERIC(12,2),
    notice_period_days INTEGER,
    years_of_experience NUMERIC(5,2),
    experience_band VARCHAR(30),
    qualification_id BIGINT,
    specialization VARCHAR(150),
    current_address_country VARCHAR(100),
    current_address_state VARCHAR(100),
    permanent_address_country VARCHAR(100),
    permanent_address_state VARCHAR(100),
    uid_number VARCHAR(100),
    employment_type_id BIGINT,
    employment_type_name VARCHAR(100),
    profile_source_id BIGINT,
    profile_source_name VARCHAR(100),
    referred_email_id VARCHAR(255),
    designation_id BIGINT,
    designation_name VARCHAR(150),
    profile_status_id BIGINT,
    profile_status_name VARCHAR(100),
    linkedin_url VARCHAR(500),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.dim_job (
    job_key BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL,
    job_code VARCHAR(50) NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    designation_id BIGINT,
    designation_name VARCHAR(150),
    department_id BIGINT,
    department_name VARCHAR(100),
    employment_type_id BIGINT,
    employment_type_name VARCHAR(100),
    salary_range VARCHAR(100),
    vacancy_count INTEGER,
    years_of_experience NUMERIC(5,2),
    experience_band VARCHAR(30),
    location_id BIGINT,
    location_name VARCHAR(150),
    work_mode_id BIGINT,
    work_mode_name VARCHAR(50),
    posted_date TIMESTAMP,
    no_of_rounds INTEGER,
    client_id BIGINT,
    client_name VARCHAR(150),
    created_by BIGINT,
    recruiter_employee_number VARCHAR(100),
    is_active BOOLEAN,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NOT NULL DEFAULT TIMESTAMP '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version_num INTEGER NOT NULL DEFAULT 1,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS ats_dwh.bridge_job_skill (
    job_key BIGINT NOT NULL,
    skill_key BIGINT NOT NULL,
    proficiency_level VARCHAR(20),
    relevant_experience NUMERIC(5,2),
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    PRIMARY KEY (job_key, skill_key),
    CONSTRAINT fk_bridge_job_skill_job FOREIGN KEY (job_key) REFERENCES ats_dwh.dim_job(job_key),
    CONSTRAINT fk_bridge_job_skill_skill FOREIGN KEY (skill_key) REFERENCES ats_dwh.dim_skill(skill_key)
);

CREATE TABLE IF NOT EXISTS ats_dwh.bridge_candidate_skill (
    candidate_key BIGINT NOT NULL,
    skill_key BIGINT NOT NULL,
    relevant_experience NUMERIC(5,2),
    last_used_year INTEGER,
    self_rating INTEGER,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    PRIMARY KEY (candidate_key, skill_key),
    CONSTRAINT fk_bridge_candidate_skill_candidate FOREIGN KEY (candidate_key) REFERENCES ats_dwh.dim_candidate(candidate_key),
    CONSTRAINT fk_bridge_candidate_skill_skill FOREIGN KEY (skill_key) REFERENCES ats_dwh.dim_skill(skill_key)
);

CREATE TABLE IF NOT EXISTS ats_dwh.fact_application (
    fact_application_key BIGSERIAL PRIMARY KEY,
    application_id BIGINT UNIQUE NOT NULL,
    application_number VARCHAR(50) UNIQUE NOT NULL,
    application_date_key INTEGER NOT NULL,
    candidate_key BIGINT NOT NULL,
    job_key BIGINT NOT NULL,
    recruiter_key BIGINT,
    application_status_key BIGINT NOT NULL,
    profile_source_key BIGINT,
    application_date TIMESTAMP NOT NULL,
    confirmed_ctc NUMERIC(12,2),
    candidate_current_ctc NUMERIC(12,2),
    candidate_expected_ctc NUMERIC(12,2),
    ctc_variance NUMERIC(12,2),
    candidate_notice_period_days INTEGER,
    candidate_years_of_experience NUMERIC(5,2),
    application_count INTEGER NOT NULL DEFAULT 1,
    is_selected INTEGER NOT NULL DEFAULT 0,
    is_rejected INTEGER NOT NULL DEFAULT 0,
    is_on_hold INTEGER NOT NULL DEFAULT 0,
    is_moved_next_round INTEGER NOT NULL DEFAULT 0,
    is_completed INTEGER NOT NULL DEFAULT 0,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    CONSTRAINT fk_fact_application_date FOREIGN KEY (application_date_key) REFERENCES ats_dwh.dim_date(date_key),
    CONSTRAINT fk_fact_application_candidate FOREIGN KEY (candidate_key) REFERENCES ats_dwh.dim_candidate(candidate_key),
    CONSTRAINT fk_fact_application_job FOREIGN KEY (job_key) REFERENCES ats_dwh.dim_job(job_key),
    CONSTRAINT fk_fact_application_recruiter FOREIGN KEY (recruiter_key) REFERENCES ats_dwh.dim_recruiter(recruiter_key),
    CONSTRAINT fk_fact_application_status FOREIGN KEY (application_status_key) REFERENCES ats_dwh.dim_application_status(application_status_key),
    CONSTRAINT fk_fact_application_source FOREIGN KEY (profile_source_key) REFERENCES ats_dwh.dim_profile_source(profile_source_key)
);

CREATE TABLE IF NOT EXISTS ats_dwh.fact_interview (
    fact_interview_key BIGSERIAL PRIMARY KEY,
    schedule_id BIGINT UNIQUE NOT NULL,
    application_number VARCHAR(50) NOT NULL,
    application_key BIGINT NOT NULL,
    scheduled_date_key INTEGER NOT NULL,
    application_stage_key BIGINT NOT NULL,
    interview_status_key BIGINT,
    interview_type_key BIGINT,
    job_key BIGINT NOT NULL,
    candidate_key BIGINT NOT NULL,
    recruiter_key BIGINT,
    scheduled_start_time TIMESTAMP NOT NULL,
    scheduled_end_time TIMESTAMP NOT NULL,
    interview_duration_minutes INTEGER NOT NULL,
    interviewer_count INTEGER NOT NULL DEFAULT 0,
    communication_rating_avg NUMERIC(5,2),
    technical_rating_avg NUMERIC(5,2),
    logical_rating_avg NUMERIC(5,2),
    overall_feedback_avg NUMERIC(5,2),
    feedback_count INTEGER NOT NULL DEFAULT 0,
    is_completed INTEGER NOT NULL DEFAULT 0,
    is_cancelled INTEGER NOT NULL DEFAULT 0,
    is_scheduled INTEGER NOT NULL DEFAULT 0,
    created_date TIMESTAMP,
    updated_date TIMESTAMP,
    CONSTRAINT fk_fact_interview_application FOREIGN KEY (application_key) REFERENCES ats_dwh.fact_application(fact_application_key),
    CONSTRAINT fk_fact_interview_date FOREIGN KEY (scheduled_date_key) REFERENCES ats_dwh.dim_date(date_key),
    CONSTRAINT fk_fact_interview_stage FOREIGN KEY (application_stage_key) REFERENCES ats_dwh.dim_application_stage(application_stage_key),
    CONSTRAINT fk_fact_interview_status FOREIGN KEY (interview_status_key) REFERENCES ats_dwh.dim_interview_status(interview_status_key),
    CONSTRAINT fk_fact_interview_type FOREIGN KEY (interview_type_key) REFERENCES ats_dwh.dim_interview_type(interview_type_key),
    CONSTRAINT fk_fact_interview_job FOREIGN KEY (job_key) REFERENCES ats_dwh.dim_job(job_key),
    CONSTRAINT fk_fact_interview_candidate FOREIGN KEY (candidate_key) REFERENCES ats_dwh.dim_candidate(candidate_key),
    CONSTRAINT fk_fact_interview_recruiter FOREIGN KEY (recruiter_key) REFERENCES ats_dwh.dim_recruiter(recruiter_key)
);

CREATE TABLE IF NOT EXISTS ats_dwh.fact_hire (
    fact_hire_key BIGSERIAL PRIMARY KEY,
    application_number VARCHAR(50) UNIQUE NOT NULL,
    application_key BIGINT NOT NULL,
    hire_date_key INTEGER NOT NULL,
    candidate_key BIGINT NOT NULL,
    job_key BIGINT NOT NULL,
    recruiter_key BIGINT,
    profile_source_key BIGINT,
    hire_date DATE NOT NULL,
    application_date TIMESTAMP NOT NULL,
    time_to_hire_days INTEGER NOT NULL,
    confirmed_ctc NUMERIC(12,2),
    current_ctc NUMERIC(12,2),
    expected_ctc NUMERIC(12,2),
    ctc_change_from_current NUMERIC(12,2),
    hire_count INTEGER NOT NULL DEFAULT 1,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_hire_application FOREIGN KEY (application_key) REFERENCES ats_dwh.fact_application(fact_application_key),
    CONSTRAINT fk_fact_hire_date FOREIGN KEY (hire_date_key) REFERENCES ats_dwh.dim_date(date_key),
    CONSTRAINT fk_fact_hire_candidate FOREIGN KEY (candidate_key) REFERENCES ats_dwh.dim_candidate(candidate_key),
    CONSTRAINT fk_fact_hire_job FOREIGN KEY (job_key) REFERENCES ats_dwh.dim_job(job_key),
    CONSTRAINT fk_fact_hire_recruiter FOREIGN KEY (recruiter_key) REFERENCES ats_dwh.dim_recruiter(recruiter_key),
    CONSTRAINT fk_fact_hire_source FOREIGN KEY (profile_source_key) REFERENCES ats_dwh.dim_profile_source(profile_source_key)
);


CREATE INDEX IF NOT EXISTS idx_dim_country_bk_current ON ats_dwh.dim_country(country_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_states_bk_current ON ats_dwh.dim_states(states_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_department_bk_current ON ats_dwh.dim_department(department_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_designation_bk_current ON ats_dwh.dim_designation(designation_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_employment_type_bk_current ON ats_dwh.dim_employment_type(employment_type_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_work_mode_bk_current ON ats_dwh.dim_work_mode(work_mode_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_gender_bk_current ON ats_dwh.dim_gender(gender_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_profile_source_bk_current ON ats_dwh.dim_profile_source(profile_source_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_profile_status_bk_current ON ats_dwh.dim_profile_status(profile_status_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_application_status_bk_current ON ats_dwh.dim_application_status(application_status_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_application_stage_bk_current ON ats_dwh.dim_application_stage(application_stage_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_interview_status_bk_current ON ats_dwh.dim_interview_status(interview_status_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_interview_type_bk_current ON ats_dwh.dim_interview_type(interview_type_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_client_bk_current ON ats_dwh.dim_client(client_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_skill_bk_current ON ats_dwh.dim_skill(skill_master_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_recruiter_bk_current ON ats_dwh.dim_recruiter(employee_number, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_candidate_bk_current ON ats_dwh.dim_candidate(candidate_number, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_job_bk_current ON ats_dwh.dim_job(job_code, is_current);

CREATE INDEX IF NOT EXISTS idx_fact_application_date_key ON ats_dwh.fact_application(application_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_application_job_key ON ats_dwh.fact_application(job_key);
CREATE INDEX IF NOT EXISTS idx_fact_application_recruiter_key ON ats_dwh.fact_application(recruiter_key);
CREATE INDEX IF NOT EXISTS idx_fact_application_source_key ON ats_dwh.fact_application(profile_source_key);
CREATE INDEX IF NOT EXISTS idx_fact_interview_date_key ON ats_dwh.fact_interview(scheduled_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_interview_job_key ON ats_dwh.fact_interview(job_key);
CREATE INDEX IF NOT EXISTS idx_fact_hire_date_key ON ats_dwh.fact_hire(hire_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_hire_job_key ON ats_dwh.fact_hire(job_key);
