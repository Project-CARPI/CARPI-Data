import mysql.connector
from mysql.connector import Error
import os
import json
from dotenv import load_dotenv


def create_connection(host_name, port, user_name, user_password, db_name):
    connection = None

    try:
        print("Attemping Connection")
        connection = mysql.connector.connect(
            host = host_name,
            port = port,
            user = user_name,
            password = user_password,
            database = db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred")

def fetch_query_results(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def get_year_sem(filename: str) -> tuple[int, str]:
    year = int(filename[0:4])
    match (filename[4:6]):
        case "01":
            return (year, "Spring")
        case "05":
            return (year, "Summer")
        case "09":
            return (year, "Fall")
    raise ValueError("Invalid Semester")

def insert_course_data(connection, data):
    SQL_DATA = "INSERT INTO course (dept, code_num, title, desc_text, credit_min, credit_max) VALUES "
    for DEPARTMENT in data:
        for COURSE in data[DEPARTMENT]["courses"]:
            COURSE_DATA = data[DEPARTMENT]["courses"][COURSE]
            CODE_NUM = COURSE.split(" ")[1]
            TITLE = COURSE_DATA["course_name"].replace("'", "\\'")
            COURSE_DETAIL = COURSE_DATA["course_detail"]

            DESCRIPTION = COURSE_DETAIL['description'].replace("'", "\\'")
            MIN_CREDIT = COURSE_DETAIL['credits']['min']
            MAX_CREDIT = COURSE_DETAIL['credits']['max']

            SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', '{TITLE}', '{DESCRIPTION}', '{MIN_CREDIT}', '{MAX_CREDIT}'),"

    SQL_DATA = SQL_DATA[:-1] + " ON DUPLICATE KEY UPDATE dept = dept;"
    execute_query(connection, SQL_DATA)
    
def insert_course_seats_data(connection, filename, data):
    SQL_DATA = "INSERT INTO course_seats (sem_year, semester, dept, code_num, seats_filled, seats_total) VALUES "
    
    SEMESTER_YEAR, SEMESTER = get_year_sem(filename)
    for DEPARTMENT in data:
        for COURSE in data[DEPARTMENT]["courses"]:
            COURSE_DATA = data[DEPARTMENT]["courses"][COURSE]
            CODE_NUM = COURSE.split(" ")[1]
            COURSE_DETAIL = COURSE_DATA["course_detail"]
            
            FILLED_SEATS = 0
            TOTAL_SEATS = 0
            for SECTION in COURSE_DETAIL["sections"]:
                FILLED_SEATS += SECTION["registered"]
                TOTAL_SEATS += SECTION["capacity"]

            SQL_DATA += f"({SEMESTER_YEAR}, '{SEMESTER}', '{DEPARTMENT}', {CODE_NUM}, {FILLED_SEATS}, {TOTAL_SEATS}),"
    SQL_DATA = SQL_DATA[:-1] + ";"
    execute_query(connection, SQL_DATA)

def insert_professor_data(connection, filename, data):
    SQL_DATA = "INSERT INTO professor (sem_year, semester, dept, code_num, prof_name) VALUES "
    
    SEMESTER_YEAR, SEMESTER = get_year_sem(filename)
    for DEPARTMENT in data:
        for COURSE in data[DEPARTMENT]["courses"]:
            COURSE_DATA = data[DEPARTMENT]["courses"][COURSE]
            CODE_NUM = COURSE.split(" ")[1]
            COURSE_DETAIL = COURSE_DATA["course_detail"]
            
            for SECTION in COURSE_DETAIL["sections"]:
                for PROFESSOR_NAME in SECTION["instructor"]:
                    PROFESSOR_NAME = PROFESSOR_NAME.replace("'", "\\'")
                    SQL_DATA += f"({SEMESTER_YEAR}, '{SEMESTER}', '{DEPARTMENT}', {CODE_NUM}, '{PROFESSOR_NAME}'),"

    SQL_DATA = SQL_DATA[:-1] + " ON DUPLICATE KEY UPDATE dept = dept;"
    execute_query(connection, SQL_DATA)

def insert_course_relationship(connection, data, year):
    SQL_DATA = "INSERT INTO course_relationship (dept, code_num, relationship, rel_dept, rel_code_num) VALUES "
    for DEPARTMENT in data:
        for COURSE in data[DEPARTMENT]["courses"]:
            COURSE_DATA = data[DEPARTMENT]["courses"][COURSE]
            CODE_NUM = COURSE.split(" ")[1]
            COURSE_DETAIL = COURSE_DATA["course_detail"]
            COREQUISITES = COURSE_DETAIL["corequisite"]
            CROSSLISTS = COURSE_DETAIL["crosslist"]

            if(CROSSLISTS != []):
                for crosslist in CROSSLISTS:
                    with open('crosslist.txt', 'a') as f:
                        f.write(f"{year} {DEPARTMENT}-{CODE_NUM} -> {crosslist}\n")
                        dept, code = crosslist.split(" ")

                    SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Cross', '{dept}', '{code}'),"

            if(COREQUISITES != []):
                for corequisite in COREQUISITES:
                    with open('corequisite.txt', 'a') as f:
                        f.write(f"{year} {DEPARTMENT}-{CODE_NUM} -> {corequisite}\n")
                    # SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Coreq', '{corequisite[0]}', '{corequisite[1]}'),"

    SQL_DATA = SQL_DATA[:-1] + "ON DUPLICATE KEY UPDATE dept = dept;"
    execute_query(connection, SQL_DATA)

def insert_course_attributes(connetion, data):
    SQL_DATA = "INSERT INTO course_attribute (dept, code_num, attr) VALUES "
    for DEPARTMENT in data:
        for COURSE in data[DEPARTMENT]["courses"]:
            COURSE_DATA = data[DEPARTMENT]["courses"][COURSE]
            CODE_NUM = COURSE.split(" ")[1]
            COURSE_DETAIL = COURSE_DATA["course_detail"]
            ATTRIBUTES = COURSE_DETAIL["attributes"]
            for attribute in ATTRIBUTES:
                SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', '{attribute}'),"
    SQL_DATA = SQL_DATA[:-1] + "ON DUPLICATE KEY UPDATE dept = dept;"
    execute_query(connection, SQL_DATA)

def insert_course_restriction(connection, data):
    SQL_DATA = "INSERT INTO course_restriction (dept, code_num, category, restr_rule, restriction) VALUES "
    for DEPARTMENT in data:
        for COURSE in data[DEPARTMENT]["courses"]:
            COURSE_DATA = data[DEPARTMENT]["courses"][COURSE]
            CODE_NUM = COURSE.split(" ")[1]
            COURSE_DETAIL = COURSE_DATA["course_detail"]
            RESTRICTIONS = COURSE_DETAIL["restrictions"]
            for restriction in RESTRICTIONS:
                # Major Restrictions
                for major in RESTRICTIONS["major"]:
                   SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Major', 'Must be', '{major}'),"
                for not_major in RESTRICTIONS["not_major"]:
                   SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Major', 'May not be', '{major}'),"
                
                # Level Restrictions
                for level in RESTRICTIONS["level"]:
                   SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Level', 'Must be', '{level}'),"
                for not_level in RESTRICTIONS["not_level"]:
                   SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Level', 'May not be', '{not_level}'),"

                # Classification Restrictions
                for classification in RESTRICTIONS["classification"]:
                   SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Classification', 'Must be', '{classification}'),"
                for not_classification in RESTRICTIONS["not_classification"]:
                   SQL_DATA += f"('{DEPARTMENT}', '{CODE_NUM}', 'Classification', 'May not be', '{not_classification}'),"

    SQL_DATA = SQL_DATA[:-1] + "ON DUPLICATE KEY UPDATE dept = dept;"

    execute_query(connection, SQL_DATA)

def main():
    load_dotenv()
    print("ENV LOADED")
    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    USER = os.getenv("USERNAME")
    PASS = os.getenv("PASS")
    DB = os.getenv("DB")

    print("Connecting to DB")
    connection = create_connection(HOST, PORT, USER, PASS, DB)

    for files in os.walk('Data'):
        sorted_files = sorted(files[2], reverse=True)
        for file in sorted_files:
            print("File: " + file)
            with open(f'Data/{file}') as f:
                data = json.load(f)
                
                print(f"    Inserting Course Data for {file}")
                insert_course_data(connection, data)
                
                print(f"    Inserting Course Seats Data for {file}")
                insert_course_seats_data(connection, file, data)

                print(f"    Inserting Professor Data for {file}")
                insert_professor_data(connection, file, data)

                # print(f"    Inserting Course Relationship Data for {file}")
                # insert_course_relationship(connection, data, file)

                print(f"    Inserting Course Attributes for {file}")
                insert_course_attributes(connection, data)
                
                print(f"    Inserting Course Restrictions for {file}")
                insert_course_restriction(connection, data)

if __name__ == "__main__":
    main()