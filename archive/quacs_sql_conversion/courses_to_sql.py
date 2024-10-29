import sys
import os
import json
import unidecode

if __name__ == '__main__':
    file_to_read = ""
    if len(sys.argv) < 2:
        file_to_read = input("Path to JSON file: ")
    elif len(sys.argv) > 2:
        print("Too many arguments")
        exit(0)
    else:
        file_to_read = sys.argv[1]
    sql_start = "INSERT INTO courses (dept, code_num, title, desc_text) VALUES\n\t"
    sql_mid = ""
    sql_end = ";"

    with open(file_to_read, 'r') as json_file:
        json_data = json.load(json_file)
        for item in json_data:
            crse = json_data[item]
            description = unidecode.unidecode(crse["description"]).replace("'", "\\'")
            if len(description) == 0:
                description = "NULL"
            else:
                description = "'" + description + "'"
            sql_mid += "('" + unidecode.unidecode(crse["subj"]).replace("'", "\\'") + "', " + unidecode.unidecode(crse["crse"]).replace("'", "\\'") + ", '" + unidecode.unidecode(crse["name"]).replace("'", "\\'") + "', " + description + "),\n\t"
        sql_mid = sql_mid[:-3]
    with open(os.path.dirname(os.path.abspath(__file__)) + "/data_insertion_sql/crse_insert.sql", "w",) as sql_file:
        sql_file.write(sql_start + sql_mid + sql_end)
