import json
import re
import sys

def parse_json(file_name):
    with open(file_name, 'r') as f:
        data = json.load(f)
    matched_instructors = []
    for item in data: # data is outermost list, item is department dictionary
        if 'courses' in item:
            courses = item['courses']
            for crse in courses: # courses is list of courses, crse is dictionary for one course
                if 'sections' in crse:
                    sections = crse['sections']
                    for sctn in sections: # sections is list of sections, sctn is dictionary for one section
                        if 'timeslots' in sctn:
                            timeslots = sctn['timeslots']
                            for tmslt in timeslots: # timeslots is list of timeslots, tmslt is dictionary for one timeslot
                                if 'instructor' in tmslt:
                                    instructor = tmslt['instructor']
                                    # matched_instructors.extend(re.findall('[^a-zA-Z ]', instructor))
                                    if (re.search('\'|-|\.', instructor)):
                                        matched_instructors.append(instructor)
    return matched_instructors

parsed_json = parse_json(sys.argv[1])

names = []
for teacher in parsed_json:
    for name in teacher.split(','):
        names.append(name.strip())

for teacher in sorted(set(names)):
    print(names.count(teacher), teacher)

# for teacher in parsed_json:
    # print(teacher)