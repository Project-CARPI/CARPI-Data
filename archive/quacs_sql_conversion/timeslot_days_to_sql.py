import sys
import os
import json

if __name__ == '__main__':
    file_to_read = ""
    if len(sys.argv) < 2:
        file_to_read = input("Path to JSON file: ")
    elif len(sys.argv) > 2:
        print("Too many arguments")
        exit(0)
    else:
        file_to_read = sys.argv[1]
    sql_start = "INSERT INTO timeslot_days (crn, time_id, week_day) VALUES\n\t"
    sql_mid = ""
    sql_end = ";"

    with open(file_to_read, 'r') as f:
        json_data = json.load(f)
        for dept in json_data:
            courses = dept['courses']
            for crse in courses:
                sections = crse['sections']
                for sctn in sections:
                    timeslots = sctn['timeslots']
                    for i, tmslt in enumerate(timeslots):
                        days = tmslt['days']
                        for day in days:
                            sql_mid += "(" + str(sctn['crn']) + ", " + str(i) + ", '" + day + "'),\n\t"
        sql_mid = sql_mid[:-3]
    with open(os.path.dirname(os.path.abspath(__file__)) + "/data_insertion_sql/tmslt_days_insert.sql", "w",) as sql_file:
        sql_file.write(sql_start + sql_mid + sql_end)
