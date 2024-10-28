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
    sql_start = "INSERT INTO departments (dept_code, dept_name, school_name) VALUES\n\t"
    sql_mid = ""
    sql_end = ";"
    
    with open(file_to_read, 'r') as json_file:
        json_data = json.load(json_file)
        for school in json_data:
            for dept in school["depts"]:
                sql_mid += "('" + dept["code"] + "', '" + dept["name"] + "', '" + school["name"] + "'),\n\t"
        sql_mid = sql_mid[:-3]
    with open(os.path.dirname(os.path.abspath(__file__)) + "/data_insertion_sql/dept_insert.sql", "w") as sql_file:
        sql_file.write(sql_start + sql_mid + sql_end)
