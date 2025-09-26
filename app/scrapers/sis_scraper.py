import asyncio
import json
import time
from enum import Enum

import aiohttp
import bs4


class ClassColumn(str, Enum):
    COURSE_TITLE = "courseTitle"
    SUBJECT_DESCRIPTION = "subjectDescription"
    COURSE_NUMBER = "courseNumber"
    SECTION = "sequenceNumber"
    CRN = "courseReferenceNumber"
    TERM = "term"


async def get_subjects(
    session: aiohttp.ClientSession, term: str
) -> list[dict[str, str]]:
    """
    Fetches the list of subjects for a given term from SIS.

    Returned data format is as follows:
    [
        {
            "code": "ADMN",
            "description": "Administrative Courses"
        },
        ...
    ]
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/classSearch/get_subject"
    params = {"term": term, "offset": 1, "max": 100}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = raw_data.replace("&amp;", "&")
    data = json.loads(raw_data)
    return data


async def reset_class_search(session: aiohttp.ClientSession, term: str) -> None:
    """
    Resets the term and subject search state on the SIS server.

    Must be called before each attempt to fetch classes from a subject in the given term.
    Otherwise, the server will continue returning the same results from the last subject
    accessed, or no data if attempting to access data from a different term.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/term/search?mode=search"
    params = {"term": term}
    async with session.get(url, params=params) as response:
        response.raise_for_status()


async def class_search(
    session: aiohttp.ClientSession,
    term: str,
    subject: str,
    max_size: int = 1000,
    sort_column: ClassColumn = ClassColumn.SUBJECT_DESCRIPTION,
    sort_asc: bool = True,
) -> list[dict]:
    """
    Fetches the list of classes for a given subject and term from SIS.

    The term and subject search state on the SIS server must be reset before each call
    to this function.

    Returned data format is very large; see docs for details.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/searchResults?pageOffset=0"
    params = {
        "txt_subject": subject,
        "txt_term": term,
        "pageMaxSize": max_size,
        "sortColumn": sort_column,
        "sortDirection": "asc" if sort_asc else "desc",
    }
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = raw_data.replace("&amp;", "&")
    data = json.loads(raw_data)
    course_data = data["data"]
    if course_data is None:
        return []
    return course_data


async def get_class_details(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getClassDetails"
    )
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        text = await response.text()
    soup = bs4.BeautifulSoup(text, "html5lib")


async def get_class_description(
    session: aiohttp.ClientSession, term: str, crn: str
) -> dict[str, str]:
    """
    Fetches and parses data from the "Course Description" tab of a class details page.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    description_data = {
        "description": "",
        "when_offered": "",
    }
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    description_tag = soup.find("section", {"aria-labelledby": "courseDescription"})
    description_text = [
        text.strip("\n").strip() for text in description_tag.text.split("\n")
    ]
    for text in description_text:
        if text.startswith("When Offered:"):
            description_data["when_offered"] = text.replace("When Offered: ", "")
        # Skip useless fields that can be obtained elsewhere
        elif text.startswith("Credit Hours:"):
            continue
        elif text.startswith("Contact, Lecture or Lab Hours:"):
            continue
        elif text.startswith("Prerequisite:"):
            continue
        elif text.startswith("Corequisite:"):
            continue
        elif text.startswith("Cross Listed:"):
            continue
        else:
            description_data["description"] += text
    return description_data


async def get_class_attributes(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionAttributes"
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_restrictions(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getRestrictions"
    )
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_prerequisites(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionPrerequisites"
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_corequisites(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCorequisites"
    )
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_crosslists(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getXlstSections"
    )
    params = {"term": term, "courseReferenceNumber": crn}


async def process_class_details(
    session: aiohttp.ClientSession, course_data: dict, class_entry: dict
) -> None:
    """
    Fetches and parses all details for a given class, populating the provided course
    data dictionary.
    """
    # Fetch class details not included in main class details
    term = class_entry["term"]
    crn = class_entry["courseReferenceNumber"]
    async with asyncio.TaskGroup() as tg:
        description_task = tg.create_task(get_class_description(session, term, crn))
        # attributes_task = tg.create_task(get_class_attributes(session, term, crn))
        # restrictions_task = tg.create_task(get_class_restrictions(session, term, crn))
        # prerequisites_task = tg.create_task(get_class_prerequisites(session, term, crn))
        # corequisites_task = tg.create_task(get_class_corequisites(session, term, crn))
        # crosslists_task = tg.create_task(get_class_crosslists(session, term, crn))

    # Wait for tasks to complete and get results
    description_data = description_task.result()
    # attributes_data = attributes_task.result()
    # restrictions_data = restrictions_task.result()
    # prerequisites_data = prerequisites_task.result()
    # corequisites_data = corequisites_task.result()
    # crosslists_data = crosslists_task.result()

    # Example course code: CSCI 1100
    course_code = f"{class_entry['subject']} {class_entry['courseNumber']}"
    if course_code not in course_data:
        course_data[course_code] = {
            "course_name": class_entry["courseTitle"],
            "course_detail": {
                "description": description_data["description"],
                "corequisite": [],
                "prerequisite": [],
                "crosslist": [],
                "attributes": [],
                "restrictions": {
                    "major": [],
                    "not_major": [],
                    "level": [],
                    "not_level": [],
                    "classification": [],
                    "not_classification": [],
                },
                "credits": {
                    "min": float("inf"),
                    "max": 0,
                },
                "offered": description_data["when_offered"],
                "sections": [],
            },
        }

    course_data = course_data[course_code]
    course_details = course_data["course_detail"]

    course_credits = course_details["credits"]
    course_credits["min"] = min(
        course_credits["min"], class_entry["creditHourLow"] or 0
    )
    course_credits["max"] = max(
        course_credits["max"], class_entry["creditHourHigh"] or 0
    )

    course_sections = course_details["sections"]
    # Use faculty RCS IDs instead of names
    section_faculty = [
        faculty_member["emailAddress"].replace("@rpi.edu", "")
        for faculty_member in class_entry["faculty"]
    ]
    course_sections.append(
        {
            "CRN": class_entry["courseReferenceNumber"],
            "instructor": section_faculty,
            "schedule": {},
            "capacity": class_entry["maximumEnrollment"],
            "registered": class_entry["enrollment"],
            "open": class_entry["seatsAvailable"],
        }
    )


async def get_course_data(
    semaphore: asyncio.Semaphore, term: str, subject: str
) -> dict:
    """
    Gets all course data for a given term and subject.

    This function spawns its own client session to avoid session state conflicts with
    other subjects that may be processing concurrently.

    In the context of this scraper, a "class" refers to a section of a course, while a
    "course" refers to the overarching course that may have multiple classes.

    The data returned from SIS is keyed by classes, not courses. This function
    manipulates and aggregates this data such that the returned structure is keyed by
    courses instead, with classes as a sub-field of each course.
    """

    async with semaphore:
        # Limit connections per session
        connector = aiohttp.TCPConnector(limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            try:
                # Reset search state on server before fetching class data
                await reset_class_search(session, term)
                class_data = await class_search(session, term, subject)
                course_data = {}
                async with asyncio.TaskGroup() as tg:
                    for entry in class_data:
                        tg.create_task(
                            process_class_details(session, course_data, entry)
                        )
                print(f"Completed processing subject: {subject}")
                return course_data
            except aiohttp.ClientError as e:
                print(f"Error processing subject {subject}: {e}")
                return {}


async def main() -> bool:
    """
    A JSESSIONID cookie is required before accessing any course data, which can be
    obtained on the first request to any SIS page. The cookie should automatically be
    included in subsequent requests made with the same aiohttp session.

    The term and subject search state on the SIS server must be reset before each attempt
    to fetch classes from a term and subject.
    """

    term = "202509"
    all_course_data = {}

    try:
        # Limit concurrent client sessions
        semaphore = asyncio.Semaphore(10)

        print("Fetching subject list...")
        async with aiohttp.ClientSession() as session:
            subjects = await get_subjects(session, term)
        print(f"Found {len(subjects)} subjects")

        # Stores all course data for a term
        all_course_data = {}

        # Process subjects in parallel, each with its own session
        tasks: list[asyncio.Task] = []
        async with asyncio.TaskGroup() as tg:
            for subject in subjects:
                subject_code = subject["code"]
                all_course_data[subject_code] = {
                    "subject_name": subject["description"],
                    "courses": {},
                }
                task = tg.create_task(get_course_data(semaphore, term, subject_code))
                tasks.append(task)

        for i, subject in enumerate(subjects):
            course_data = tasks[i].result()
            all_course_data[subject["code"]]["courses"] = course_data

        # Write all data for term to JSON file
        with open(f"{term}.json", "w") as f:
            json.dump(all_course_data, f, indent=4)

        print(f"Successfully processed {len(all_course_data)} subjects")

    except Exception as e:
        print(f"Error in main: {e}")
        import traceback

        traceback.print_exc()
        return False
    return True


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Total time elapsed: {end_time - start_time:.2f} seconds")
