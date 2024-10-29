import sys
import os
import json

def prereq(values):
    if len(values) == 0:
        raise ValueError("No values in prerequisites dictionary")
    if "nested" in values:
        return "'" + nested(values['nested'], values['type']) + "'"
    desc_str = ""
    for key in values:
        match key:
            case "course":
                desc_str += values[key] + ", "
            case "type":
                pass
            case _:
                oops("prereq", key)
    return "'" + desc_str[:-2] + "'"

def nested(values, nest_type):
    if len(values) == 0:
        raise ValueError("Nothing in nested array")
    nested_string = ""
    for arr in values:
        for key in arr:
            match key:
                case "course":
                    nested_string += arr[key] + " " + nest_type + " "
                case "nested":
                    nested_string += "(" + nested(arr['nested'], arr['type']) + ") " + nest_type + " "
                case "type":
                    pass
                case _:
                    oops("nested", key)
    return nested_string[:-(2 + len(nest_type))]

def coreq(values):
    if len(values) == 0:
        raise ValueError("No values in corequisites array")
    final_string = ""
    for course in values:
        final_string += course[:course.index("-")] + " " + course[course.index("-") + 1:] + ", "
    return "'" + final_string[:-2] + "'"

def cross_list(values):
    if len(values) == 0:
        raise ValueError("No values in cross_list_courses array")
    final_string = ""
    for course in values:
        final_string += course[:course.index("-")] + " " + course[course.index("-") + 1:] + ", "
    return "'" + final_string[:-2] + "'"

def restriction(values, arr):
    if len(values) == 0:
        raise ValueError("No values in restriction dictionary")
    for key in values:
        match key:
            case "level":
                arr[0] = "'" + could_be(values[key], key) + "'"
            case "major":
                arr[1] = "'" + could_be(values[key], key) + "'"
            case "classification":
                arr[2] = "'" + could_be(values[key], key) + "'"
            case "degree":
                arr[3] = "'" + could_be(values[key], key) + "'"
            case "field_of_study":
                arr[4] = "'" + could_be(values[key], key) + "'"
            case "campus":
                arr[5] = "'" + could_be(values[key], key) + "'"
            case "college":
                arr[6] = "'" + could_be(values[key], key) + "'"
            case _:
                oops("restriction", key)

def could_be(values, origin):
    if len(values) == 0:
        raise ValueError(f"No values in {origin} dictionary")
    desc_str = ""
    for key in values:
        if key == "must_be":
            desc_str += "Must be "
            for val in values[key]:
                desc_str += val + " or "
        elif key == "may_not_be":
            desc_str += "May not be "
            for val in values[key]:
                desc_str += val + " or "
        else:
            oops("could_be", key)
    return desc_str[:-4]

def oops(source, value):
    raise ValueError(f"Dictionary key \"{value}\" not accounted for in {source}() function")

def allnull(p, c, s, r):
    if p != "NULL" or c != "NULL" or s != "NULL":
        return False
    for val in r:
        if val != "NULL":
            return False
    return True

if __name__ == '__main__':
    file_to_read = ""
    if len(sys.argv) < 2:
        file_to_read = input("Path to JSON file: ")
    elif len(sys.argv) > 2:
        print("Too many arguments")
        exit(0)
    else:
        file_to_read = sys.argv[1]
    sql_start = "INSERT INTO prerequisites (crn, prereqs, coreqs, cross_list, restr_level, restr_major, restr_clsfctn, restr_degree, restr_field, restr_campus, restr_college) VALUES\n\t"
    sql_mid = ""
    sql_end = ";"

    with open(file_to_read, 'r') as f:
        json_data = json.load(f)
        for crn in json_data:
            section = json_data[crn]
            prq = "NULL"
            crq = "NULL"
            csl = "NULL"
            rst = ["NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"]
            for key in section:
                match key:
                    case 'prerequisites':
                        prq = prereq(section[key])
                    case 'corequisites':
                        crq = coreq(section[key])
                    case 'cross_list_courses':
                        csl = cross_list(section[key])
                    case 'restrictions':
                        restriction(section[key], rst)
                    case _:
                        oops("main", key)
            if not allnull(prq, crq, csl, rst):
                sql_mid += "(" + crn + ", " + prq + ", " + crq + ", " + csl + ", " + rst[0] + ", " + rst[1] + ", " + rst[2] + ", " + rst[3] + ", " + rst[4] + ", " + rst[5] + ", " + rst[6] + "),\n\t"
        sql_mid = sql_mid[:-3]
    with open(os.path.dirname(os.path.abspath(__file__)) + "/data_insertion_sql/prereq_insert.sql", "w",) as sql_file:
        sql_file.write(sql_start + sql_mid + sql_end)
