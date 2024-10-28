import sys
import os
import json

def process_profs(profs):
    new_profs = []
    for name in profs.split(','):
        if len(name) == 0 or name == 'TBA':
            continue
        new_profs.append(name.strip().replace("'", "\\'"))
    return new_profs

if __name__ == '__main__':
    file_to_read = ""
    if len(sys.argv) < 2:
        file_to_read = input("Path to JSON file: ")
    elif len(sys.argv) > 2:
        print("Too many arguments")
        exit(0)
    else:
        file_to_read = sys.argv[1]
    sql_start = "INSERT INTO timeslot_profs (crn, time_id, instructor) VALUES\n\t"
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
                        profs = tmslt['instructor']
                        for prof in process_profs(profs):
                            sql_mid += "(" + str(sctn['crn']) + ", " + str(i) + ", '" + prof + "'),\n\t"
        sql_mid = sql_mid[:-3]
    with open(os.path.dirname(os.path.abspath(__file__)) + "/data_insertion_sql/tmslt_profs_insert.sql", "w",) as sql_file:
        sql_file.write(sql_start + sql_mid + sql_end)
