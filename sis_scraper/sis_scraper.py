import bs4
import asyncio  
import aiohttp
import json
import time
import lxml
import re
import utils

async def get_subjects(session, term):
    subjects_dict = {}
    url = "https://sis.rpi.edu/rss/bwckctlg.p_display_courses"
    url = f"{url}?term_in={term}&sel_crse_strt=&sel_crse_end=&sel_subj=&sel_levl=&sel_schd=&sel_coll=&sel_divs=&sel_dept=&sel_attr="

    async with session.get(url) as response:
        soup = bs4.BeautifulSoup(await response.text(), "lxml")

        subjects = soup.find("select", {"name": "sel_subj"}).find_all("option")
        for subject in subjects:
            subject_code = subject.get("value")
            subject_name = subject.get_text()
            subjects_dict[subject_code] = subject_name

        return subjects_dict

async def get_courses(session, term, subject_code):   
    course_dict = {}
    url = "https://sis.rpi.edu/rss/bwckctlg.p_display_courses"
    params = f"term_in={term}&"\
            "call_proc_in=&"\
            "sel_subj=dummy&"\
            "sel_levl=dummy&"\
            "sel_schd=dummy&"\
            "sel_coll=dummy&"\
            "sel_divs=dummy&"\
            "sel_dept=dummy&"\
            "sel_attr=dummy&"\
            f"sel_subj={subject_code}&"\
            "sel_crse_strt=&"\
            "sel_crse_end=&"\
            "sel_title=&"\
            "sel_levl=%25&"\
            "sel_schd=%25&"\
            "sel_coll=%25&"\
            "sel_divs=%25&"\
            "sel_dept=%25&"\
            "sel_from_cred=&"\
            "sel_to_cred=&"\
            "sel_attr=%25"

    url = f"{url}?{params}"
    async with session.get(url) as response:
        start = time.time()
        soup = bs4.BeautifulSoup(await response.text(), "lxml")

        tasks = []

        subj_class_data = soup.find_all("td", class_="nttitle")
        for class_link in subj_class_data:
            class_link = class_link.find("a")
            class_code_name = class_link.get_text().split(" - ") # some class names include '-' 
            class_code, class_name = class_code_name[0], class_code_name[1]
            subject_code, course_code = class_code.split(" ")

            task = asyncio.create_task(get_course_detail(session, term, subject_code, course_code))
            tasks.append((task, class_code, class_name))

        results = await asyncio.gather(*[task for task, _, _ in tasks])

        for (task_result, (_, class_code, class_name)) in zip(results, tasks):
            # print(task_result)
            if task_result != None:
                course_dict[class_code] = {
                    "course_name": class_name,
                    "course_detail": task_result
                }

        end = time.time()
        print(f"Time taken to get {term} -> {subject_code}: {end - start} seconds")
        return course_dict
        
async def parse_prereqs(soup):
    # prereq_pattern = r"(?:Prerequisite(?:s)?|Prerequisiste|Prerequisite or Corequisite):\s?(.*?)(?=\bCorequisite\b|$)"
    prereq_label = soup.find('span', class_='fieldlabeltext', string='Prerequisites: ')
    
    # No prerequisites found
    if not prereq_label:
        return []
    
    prereq_info = []
    for sibling in prereq_label.next_siblings:
        if sibling.name != 'br' and sibling.get_text(strip=True):
            prereq_info.append(sibling.get_text(separator=" ", strip=True))
    prereq_text = ' '.join(prereq_info)

    prereq_text_cleaned = re.sub(r'Undergraduate level ', '', prereq_text)
    prereq_text_cleaned = re.sub(r'[()]', ' ', prereq_text_cleaned)
    prereq_text_cleaned = re.sub(r'\s+', ' ', prereq_text_cleaned).strip()

    individual_prereq = prereq_text_cleaned.split(" and ")
        
    if(individual_prereq == ""):
        return []
    return individual_prereq 

async def fetch_CRNs(soup):
    CRNs = []
    section_title_wrappers = soup.find_all("th", class_="ddtitle")
    for section_title_wrapper in section_title_wrappers:
        section_title = section_title_wrapper.find("a").contents[0]
        CRN = re.search(r"\d{5}", section_title).group()
        CRNs.append(CRN)
    return CRNs

def get_seat_info(seat_table):
    seat_info = seat_table.find_all("td")

    capacity = seat_info[1].text
    registered = seat_info[2].text
    open_seats = seat_info[3].text
    
    return capacity, registered, open_seats

def get_section_crosslist(section_body):
     # Cross List Courses
    course_pattern = r"[A-Z]{4}\s\d{4}"
    crosslist = []
    if "Cross List Courses:" in section_body:
        if "Prerequisites:" in section_body:
            crosslistBody = section_body[section_body.index("Cross List Courses:"):section_body.index("Prerequisites")]
        else:
            crosslistBody = section_body[section_body.index("Cross List Courses:"):]
        crosslist = re.findall(course_pattern, crosslistBody)

    return crosslist

def get_restrictions(section_body):
    restriction_types = ["Must be enrolled in one of the following Majors",
                         "May not be enrolled in one of the following Majors",
                         "Must be enrolled in one of the following Levels",
                         "May not be enrolled in one of the following Levels",
                         "Must be enrolled in one of the following Classifications",
                         "May not be enrolled as the following Classifications"]
                         
    major_restrictions = []
    not_major_restrictions = []
    level_restrictions = []
    not_level_restrictions = []
    classification_restrictions = []
    not_classification_restrictions = []
    
     # Remove excess characters
    section_body = section_body.replace("&nbsp;", "").replace("\xa0", "").replace("   ", "")
    while "\n\n" in section_body:
        section_body = section_body.replace("\n\n", "\n")

    if "Restrictions:" in section_body:
        if "Prerequisites:" in section_body:
            restriction_body = section_body[section_body.index("Restrictions:"):section_body.index("Prerequisites:")]
        else:
            restriction_body = section_body[section_body.index("Restrictions:"):]

        for restriction in restriction_types:
            match = re.search(rf"{restriction}:\n([^\n]*(?:\n(?!Must be|May not be)[^\n]*)*)", restriction_body)
            if match:
                restriction_data = match.group(1).strip().split("\n")

                if restriction == restriction_types[0]:
                    major_restrictions = restriction_data
                elif restriction == restriction_types[1]:
                    not_major_restrictions = restriction_data
                elif restriction == restriction_types[2]:
                    level_restrictions = restriction_data
                elif restriction == restriction_types[3]:
                    not_level_restrictions = restriction_data
                elif restriction == restriction_types[4]:
                    classification_restrictions = restriction_data
                elif restriction == restriction_types[5]:
                    not_classification_restrictions = restriction_data

    return [major_restrictions, not_major_restrictions, level_restrictions, not_level_restrictions, classification_restrictions, not_classification_restrictions]

# Gets the restrictions, crosslist, capacity, registered, open_seats given a term and CRN
async def get_section_data(session, term, CRN):
    url = "https://sis.rpi.edu/rss/bwckschd.p_disp_detail_sched"
    params = f"term_in={term}&crn_in={CRN}"
    url = f"{url}?{params}"
    async with session.get(url) as response:
        soup = bs4.BeautifulSoup(await response.text(), "lxml")  
        seat_table = soup.find("table",
        {
            "class": "datadisplaytable",
            "summary": "This layout table is used to present the seating numbers.",
        })
        section_body = soup.find("td",{"class": "dddefault"}).getText()

        section_crosslist = get_section_crosslist(section_body)
        restrictions = get_restrictions(section_body)
        capacity, registered, open_seats = get_seat_info(seat_table)

    return section_crosslist, restrictions, capacity, registered, open_seats

async def get_section_info(session, term, soup):
    CRNs = await fetch_CRNs(soup)  

    section_tables = soup.find_all("table",{
            "class": "datadisplaytable",
            "summary": "This table lists the scheduled meeting times and assigned instructors for this class..",
        },
    )

    sections_data = []
    crosslist = []
    major_restrictions, level_restrictions, classification_restrictions = [], [], []
    not_major_restrictions, not_level_restrictions, not_classification_restrictions = [], [], []

    restrictions = {
        "major": major_restrictions,
        "not_major": not_major_restrictions,
        "level": level_restrictions,
        "not_level": not_level_restrictions,
        "classification": classification_restrictions,
        "not_classification": not_classification_restrictions
    }
    
    count = 0
    
    #per section
    for section_table in section_tables:
        schedule = {
            "monday": {},
            "tuesday": {},
            "wednesday": {},
            "thursday": {},
            "friday": {},
            "saturday": {},
            "other": {},
        }
        instructors = []

        #per row in section
        for section_row in section_table.find_all('tr')[1:]:  # Skip the header row
            #per cell in row
            section_row_info = [section_cells.text for section_cells in section_row.find_all("td")]

            time = section_row_info[1]
            days = section_row_info[2]
            location = section_row_info[3]
            if section_row_info[-1] != "TBA":
                instructors_string = utils.clean_instructors(section_row_info[-1])
                instructors = instructors_string.split(", ")

            days = days.replace(u'\xa0', u' ')

            for day in utils.map_day_codes_to_days(days):
                day_info = {
                    "time": time,
                    "location": location
                }
                schedule[day] = day_info

        CRN = CRNs[count]

        section_crosslist, restrictions, capacity, registered, open_seats = await get_section_data(session, term, CRN)
        # Crosslist
        if(section_crosslist != None):
            for course in section_crosslist:
                if course not in crosslist:
                    crosslist.append(course)

        section_major_restrictions = restrictions[0]
        section_not_major_restrictions = restrictions[1]
        section_level_restrictions = restrictions[2]
        section_not_level_restrictions = restrictions[3]
        section_classification_restrictions = restrictions[4]
        section_not_classification_restrictions = restrictions[5]

        # Restrictions
        if section_major_restrictions != []:
            for restriction in section_major_restrictions:
                if restriction not in major_restrictions:
                    major_restrictions.append(restriction)
        
        if section_not_major_restrictions != []:
            for restriction in section_not_major_restrictions:
                if restriction not in not_major_restrictions:
                    not_major_restrictions.append(restriction)

        if section_level_restrictions != []:
            for restriction in section_level_restrictions:
                if restriction not in level_restrictions:
                    level_restrictions.append(restriction)
        if section_not_level_restrictions != []:
            for restriction in section_not_level_restrictions:
                if restriction not in not_level_restrictions:
                    not_level_restrictions.append(restriction)

        if section_classification_restrictions != []:
            for restriction in section_classification_restrictions:
                if restriction not in classification_restrictions:
                    classification_restrictions.append(restriction)
        if section_not_classification_restrictions != []:
            for restriction in section_not_classification_restrictions:
                if restriction not in not_classification_restrictions:
                    not_classification_restrictions.append(restriction)
        

        restrictions = {
            "major": major_restrictions,
            "not_major": not_major_restrictions,
            "level": level_restrictions,
            "not_level": not_level_restrictions,
            "classification": classification_restrictions,
            "not_classification": not_classification_restrictions
        }

        section_entry = {
            "CRN": CRN,
            "instructor": instructors,
            "schedule": schedule,
            "capacity": int(capacity),
            "registered": int(registered),
            "open": int(open_seats)
        }
        
        sections_data.append(section_entry)
        count += 1

    return crosslist, restrictions, sections_data

async def fetch_crosslist_restriction_section(session, term, subject_code, course_code):
  url = "https://sis.rpi.edu/rss/bwckctlg.p_disp_listcrse"
  param = f"term_in={term}&subj_in={subject_code}&crse_in={course_code}&schd_in=L"
  url = f"{url}?{param}"
  async with session.get(url) as response:
    soup = bs4.BeautifulSoup(await response.text(), "lxml")

    is_section_found = soup.find("caption", class_="captiontext")
    if(is_section_found == None):
        return None, None, None

    response_crosslist, response_restrictions, sections_data = await get_section_info(session, term, soup)

    return response_crosslist, response_restrictions, sections_data
  
async def parse_attributes(soup, term, subject_code, course_code):
    attributes = []
    attribute_label = soup.find('span', class_='fieldlabeltext', string='Course Attributes: ')

    if not attribute_label:
        return attributes
    
    attributes = attribute_label.next.next.next.strip().split(", ")

    return attributes

async def parse_crosslist(soup):
    soup.find('span', class_='fieldlabeltext', string='Cross Listed: ')
        
async def get_course_detail(session, term, subject_code, course_code):
    url = "https://sis.rpi.edu/rss/bwckctlg.p_disp_course_detail"
    params = f"cat_term_in={term}&subj_code_in={subject_code}&crse_numb_in={course_code}"
    url = f"{url}?{params}"
    info = {
        "description" : "",
        "corequisite" : [],
        "prerequisite" : [],
        "crosslist" : [],
        "attributes" : [],
        "restrictions" : {
            "major": [],
            "level": [],
            "classification": [],
            "grade": []
        },
        "credits" : {
            "min": 0,
            "max": 0
        },
        "offered" : "",
    }
    
    async with session.get(url) as response:
        soup = bs4.BeautifulSoup(await response.text(), "lxml")
        
        course_data = soup.find("td", class_="ntdefault").contents[0].strip().split("\n")

        crosslist, restrictions, sections_data = await fetch_crosslist_restriction_section(session, term, subject_code, course_code)

        if(sections_data == None):
            return None

        coreq_pattern = r"Corequisite:\s?(.*)"

        for _ in course_data:
            if _ != "":
                if(info["description"] == ""): # if not empty
                    info["description"] = _.strip()
                else:
                    if("Corequisite:" in _):
                        try:
                            if("Corequisite:" in _):
                                coreq_match = re.search(coreq_pattern, _, re.IGNORECASE)
                                info["corequisite"] = [coreq_match.group(1).strip()]
                        except OSError as e:
                            print(e)
                            print(f"ERROR Coreq: {subject_code} - {course_code}")
                            info["corequisite"] = None
                    elif("Prerequisite:" in _):
                        try:
                            info["prerequisite"] = await parse_prereqs(soup)
                        except OSError as e:
                            print(e)
                            print(f"ERROR Prereq: {subject_code} - {course_code}")
                            info["prerequisite"] = None
                    elif("Credit Hours:" in _):
                        try:
                            min_max = utils.get_min_max(_.split(":")[1].strip())
                            info["credits"] = {
                                "min": min_max[0],
                                "max": min_max[1]
                            } 
                        except OSError as e:
                            print(e)
                            print(f"ERROR Credit: {subject_code} - {course_code}")
                            info["credits"] = None
                    elif("When Offered: " in _):
                        try:
                            info["offered"] = _.split(":")[1].strip()
                        except OSError as e:
                            print(e)
                            print(f"ERROR Offered: {subject_code} - {course_code}")
                            info["offered"] = None
                    elif("Cross Listed:" in _):
                        try:
                            info["crosslist"] = [_.split(":")[1].strip()]
                        except OSError as e:
                            print(e)
                            print(f"ERROR crosslist: {subject_code} - {course_code}")
                            info["crosslist"] = None

        info["crosslist"] = crosslist
        info["attributes"] = await parse_attributes(soup, term, subject_code, course_code)
        info["restrictions"] = restrictions
        info["sections"] = sections_data 

    return info

async def main():
    total_start = time.time()
    start_year = 2023
    end_year = 2024
    for i in range(start_year, end_year + 1):
        for semester in ["fall", "spring", "summer"]:
            term = utils.get_term(i, semester)
            print(f"Running Term: {term}")
            async with aiohttp.ClientSession() as session:
                start = time.time() 
                subjects = await get_subjects(session, term=term)
                all_courses = {}

                tasks = []

                subject_metadata = []
                for subject_code, subject_name in subjects.items():
                    task = asyncio.create_task(get_courses(session=session, term=term, subject_code=subject_code))
                    tasks.append(task)
                    subject_metadata.append((subject_code, subject_name))

                courses_by_subject = await asyncio.gather(*tasks)
                for (subject_code, subject_name), courses in zip(subject_metadata, courses_by_subject):
                    all_courses[subject_code] = {
                        "subject_name": subject_name,
                        "courses": courses
                    }
                with open(f"./data/{term}.json", "w") as f:
                    json.dump(all_courses, f, indent=4)
                end = time.time()
                print(f"Time taken for {term}: {end - start} seconds")

    total_end = time.time()
    print(f"Total time taken Total: {total_end - total_start} seconds")

if __name__ == "__main__":
    asyncio.run(main())