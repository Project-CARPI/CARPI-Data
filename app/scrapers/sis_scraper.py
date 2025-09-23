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
        data = await response.json()
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
) -> list[dict[str, str]]:
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
        data = await response.json()
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


async def get_class_description(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_attributes(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionAttributes"
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_restrictions(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getRestrictions"
    )
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_corequisites(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCorequisites"
    )
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_prerequisites(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionPrerequisites"
    params = {"term": term, "courseReferenceNumber": crn}


async def get_class_crosslists(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getXlstSections"
    )
    params = {"term": term, "courseReferenceNumber": crn}


async def get_course_data(
    session: aiohttp.ClientSession, term: str, subject: str
) -> list[dict]:
    """
    Gets all course data for a given term and subject.

    In the context of this scraper, a "class" refers to a section of a course, while a
    "course" refers to the overarching course that may have multiple classes.

    The data returned from SIS is keyed by classes, not courses. This function
    manipulates and aggregates this data such that the returned structure is keyed by
    courses instead, with classes as a sub-field of each course.

    Think of this as a main function that calls all other helper functions and aggregates
    all the data into a single, manageable structure.
    """
    class_data = await class_search(session, term, subject)
    parsed_data = {}
    for entry in class_data:
        # Example course code: CSCI 1100
        course_code = f"{entry['subject']} {entry['courseNumber']}"
        if course_code not in parsed_data:
            # TODO: Fill all details except credits, offered, and sections on initial
            # parse. Aggregate data for the other fields as loop continues.
            parsed_data[course_code] = {
                "course_name": entry["courseTitle"],
                "course_detail": {
                    "description": "",
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
                        "min": entry["creditHourLow"],
                        "max": entry["creditHourHigh"],
                    },
                    "offered": "",
                    "sections": [
                        {
                            "CRN": entry["courseReferenceNumber"],
                            "instructor": [
                                faculty_member["displayName"]
                                for faculty_member in entry["faculty"]
                            ],
                            "schedule": {},
                            "capacity": entry["maximumEnrollment"],
                            "registered": entry["enrollment"],
                            "open": entry["seatsAvailable"],
                        }
                    ],
                },
            }

        parsed_course_data = parsed_data[course_code]
        parsed_course_details = parsed_course_data["course_detail"]

        parsed_course_credits = parsed_course_details["credits"]
        parsed_course_credits["min"] = min(
            parsed_course_credits["min"], entry["creditHourLow"]
        )
        parsed_course_credits["max"] = max(
            parsed_course_credits["max"], entry["creditHourHigh"]
        )

    return parsed_data


async def main():
    """
    A JSESSIONID cookie is required before accessing any course data, which can be
    obtained on the first request to any SIS page. The cookie should automatically be
    included in subsequent requests made with the same aiohttp session.

    reset_course_search() must be called before each new attempt to fetch sections from
    a different subject.
    """
    async with aiohttp.ClientSession() as session:
        term = "202409"
        subjects = await get_subjects(session, term)
        await reset_class_search(session, term)
        data = await class_search(session, term, subjects[0]["code"])
        print(json.dumps(data, indent=4))
    return True


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Total time elapsed: {end_time - start_time:.2f} seconds")
