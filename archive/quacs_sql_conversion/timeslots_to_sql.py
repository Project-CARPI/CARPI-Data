import sys
import os
import json
    
def process_location(place):
    if len(place) == 0 or place == 'TBA':
        return 'NULL'
    else:
        return "'" + place + "'"

def process_time(num):
    if num == -1:
        return 'NULL'
    else:
        return str(num) + "00"
    
def process_date(date):
    if len(date) == 0:
        return 'NULL'
    else:
        return "'0000-" + date[0:2] + "-" + date[3:5] + "'"

if __name__ == '__main__':
    file_to_read = ""
    if len(sys.argv) < 2:
        file_to_read = input("Path to JSON file: ")
    elif len(sys.argv) > 2:
        print("Too many arguments")
        exit(0)
    else:
        file_to_read = sys.argv[1]
    sql_start = "INSERT INTO timeslots (crn, time_id, room, date_start, date_end, time_start, time_end) VALUES\n\t"
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
                        sql_mid += "(" + str(sctn['crn']) + ", " + str(i) + ", " + process_location(tmslt['location']) + ", " + process_date(tmslt['dateStart']) + ", " + process_date(tmslt['dateEnd']) + ", " + process_time(tmslt['timeStart']) + ", " + process_time(tmslt['timeEnd']) + "),\n\t"
        sql_mid = sql_mid[:-3]
    with open(os.path.dirname(os.path.abspath(__file__)) + "/data_insertion_sql/tmslt_insert.sql", "w",) as sql_file:
        sql_file.write(sql_start + sql_mid + sql_end)
