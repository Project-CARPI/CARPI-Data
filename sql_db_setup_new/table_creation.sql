-- drop existing tables to be replaced
DROP TABLE IF EXISTS course_attribute;
DROP TABLE IF EXISTS course_seats;
DROP TABLE IF EXISTS professor;
DROP TABLE IF EXISTS course_restriction;
DROP TABLE IF EXISTS prereq_course;
DROP TABLE IF EXISTS prereq_nesting;
DROP TABLE IF EXISTS course_relationship;
DROP TABLE IF EXISTS course;

-- create the tables
CREATE TABLE IF NOT EXISTS course(
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    title VARCHAR(255) NOT NULL,
    desc_text TEXT NOT NULL,
    credit_min TINYINT NOT NULL,
    credit_max TINYINT NOT NULL,
    CONSTRAINT pk_course
        PRIMARY KEY(dept, code_num)
) COMMENT 'Courses from the RPI catalog';

CREATE TABLE IF NOT EXISTS course_relationship(
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    relationship ENUM('Coreq', 'Cross') NOT NULL,
    rel_dept VARCHAR(4) NOT NULL,
    rel_code_num SMALLINT NOT NULL,
    CONSTRAINT pk_course_rel
        PRIMARY KEY(dept, code_num, relationship, rel_dept, rel_code_num),
    CONSTRAINT fk_course_rel_course
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num)
) COMMENT 'Courses that are corequisites or crosslists for other courses';

CREATE TABLE IF NOT EXISTS prereq_nesting(
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    nesting_id TINYINT NOT NULL,
    relationship ENUM('AND', 'OR'),
    parent_id TINYINT,
    CONSTRAINT pk_prereq_nesting
        PRIMARY KEY(dept, code_num, nesting_id),
    CONSTRAINT fk_prereq_nesting_cours
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num),
    CONSTRAINT fk_prereq_nesting_parent
        FOREIGN KEY(dept, code_num, parent_id)
        REFERENCES prereq_nesting(dept, code_num, nesting_id)
) COMMENT 'The structure of prerequisite nesting for each course';


CREATE TABLE IF NOT EXISTS prereq_course(
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    parent_id TINYINT NOT NULL,
    req_dept VARCHAR(4) NOT NULL,
    req_code_num SMALLINT NOT NULL,
    CONSTRAINT pk_prereq_course
        PRIMARY KEY(dept, code_num, parent_id, req_dept, req_code_num),
    CONSTRAINT fk_prereq_course_course
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num),
    CONSTRAINT fk_prereq_course_nesting
        FOREIGN KEY(dept, code_num, parent_id)
        REFERENCES prereq_nesting(dept, code_num, nesting_id)
) COMMENT 'Prerequisites for other courses, nested according to prereq_structure';

CREATE TABLE IF NOT EXISTS course_restriction(
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    category ENUM(
        'Major',
        'Level',
        'Classification'
    ) NOT NULL,
    restr_rule ENUM('Must be', 'May not be') NOT NULL,
    restriction VARCHAR(255) NOT NULL,
    CONSTRAINT pk_course_restriction
        PRIMARY KEY(dept, code_num, category, restr_rule, restriction),
    CONSTRAINT fk_course_restriction_course
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num)
) COMMENT 'Restrictions for each course';

CREATE TABLE IF NOT EXISTS professor(
    sem_year SMALLINT NOT NULL,
    semester ENUM('Fall', 'Spring', 'Summer') NOT NULL,
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    prof_name VARCHAR(255) NOT NULL,
    CONSTRAINT pk_professor
        PRIMARY KEY(sem_year, semester, dept, code_num, prof_name),
    CONSTRAINT fk_professor_course
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num)
) COMMENT 'Professors for each course';

CREATE TABLE IF NOT EXISTS course_seats(
    sem_year SMALLINT NOT NULL,
    semester ENUM('Fall', 'Spring', 'Summer') NOT NULL,
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    seats_filled SMALLINT NOT NULL,
    seats_total SMALLINT NOT NULL,
    CONSTRAINT pk_course_seats
        PRIMARY KEY(sem_year, semester, dept, code_num),
    CONSTRAINT fk_course_seats_course
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num)
) COMMENT 'Seats filled per semester for each course';

CREATE TABLE IF NOT EXISTS course_attribute(
    dept VARCHAR(4) NOT NULL,
    code_num SMALLINT NOT NULL,
    attr ENUM(
        'Communication Intensive',
        'Culminating Exp/Capstone',
        'Data Intensive I',
        'Data Intensive II',
        'Writing Intensive',
        'HASS Inquiry',
        'Introductory Level Course',
        'PDII Option for Engr Majors'
    ) NOT NULL,
    CONSTRAINT pk_course_attribute
        PRIMARY KEY(dept, code_num, attr),
    CONSTRAINT fk_course_attribute_course
        FOREIGN KEY(dept, code_num)
        REFERENCES course(dept, code_num)
) COMMENT 'Attributes for each course';
