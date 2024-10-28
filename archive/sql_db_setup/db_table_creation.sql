CREATE TABLE IF NOT EXISTS departments(
    dept_code VARCHAR(255) NOT NULL,
    dept_name VARCHAR(255) NOT NULL,
    school_name VARCHAR(255) NOT NULL,
    CONSTRAINT pk_departments PRIMARY KEY(dept_code)
) COMMENT 'Data about RPI departments and corresponding schools';

CREATE TABLE IF NOT EXISTS courses(  
    dept VARCHAR(255) NOT NULL,
    code_num INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    desc_text TEXT,
    CONSTRAINT pk_courses PRIMARY KEY(dept, code_num)
) COMMENT 'Courses from the RPI catalog';

CREATE TABLE IF NOT EXISTS sections(
    crn INT NOT NULL,
    dept VARCHAR(255) NOT NULL,
    code_num INT NOT NULL,
    sec_code VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    seat_max INT NOT NULL,
    credit_min INT NOT NULL,
    credit_max INT NOT NULL,
    CONSTRAINT pk_sections PRIMARY KEY(crn),
    CONSTRAINT fk_sections_courses FOREIGN KEY(dept, code_num) REFERENCES courses(dept, code_num)
) COMMENT 'Sections for each course';

CREATE TABLE IF NOT EXISTS prerequisites(
    crn INT NOT NULL,
    prereqs TEXT,
    coreqs TEXT,
    cross_list TEXT,
    restr_level TEXT,
    restr_major TEXT,
    restr_clsfctn TEXT,
    restr_degree TEXT,
    restr_field TEXT,
    restr_campus TEXT,
    restr_college TEXT,
    CONSTRAINT pk_prerequisites PRIMARY KEY(crn)
) COMMENT 'Prerequisites for each section';

CREATE TABLE IF NOT EXISTS timeslots(
    crn INT NOT NULL,
    time_id INT NOT NULL,
    room VARCHAR(255),
    date_start DATE,
    date_end DATE,
    time_start TIME,
    time_end TIME,
    CONSTRAINT pk_timeslots PRIMARY KEY(crn, time_id),
    CONSTRAINT fk_timeslots_sections FOREIGN KEY(crn) REFERENCES sections(crn)
) COMMENT 'Timeslot data for each section';

CREATE TABLE IF NOT EXISTS timeslot_days(
    crn INT NOT NULL,
    time_id INT NOT NULL,
    week_day VARCHAR(1) NOT NULL,
    CONSTRAINT pk_slot_days PRIMARY KEY(crn, time_id, week_day),
    CONSTRAINT fk_slot_days_timeslots FOREIGN KEY(crn, time_id) REFERENCES timeslots(crn, time_id)
) COMMENT 'Which days each section timeslot occurs on';

CREATE TABLE IF NOT EXISTS timeslot_profs(
    crn INT NOT NULL,
    time_id INT NOT NULL,
    instructor VARCHAR(255) NOT NULL,
    CONSTRAINT pk_slot_profs PRIMARY KEY(crn, time_id, instructor),
    CONSTRAINT fk_slot_profs_timeslots FOREIGN KEY(crn, time_id) REFERENCES timeslots(crn, time_id)
) COMMENT 'Which professors teach each section timeslot';
