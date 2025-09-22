import asyncio
import json
import time
from enum import Enum

import aiohttp
import bs4


class CourseColumn(str, Enum):
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


async def reset_course_search(session: aiohttp.ClientSession, term: str) -> None:
    """
    Resets the term and subject search state on the SIS server.

    Must be called before each attempt to fetch courses from a subject in the given term.
    Otherwise, the server will continue returning the same results from the last subject
    accessed, or no data if attempting to access data from a different term.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/term/search?mode=search"
    params = {"term": term}
    async with session.get(url, params=params) as response:
        response.raise_for_status()


async def course_search(
    session: aiohttp.ClientSession,
    term: str,
    subject: str,
    max_size: int = 1000,
    sort_column: CourseColumn = CourseColumn.SUBJECT_DESCRIPTION,
    sort_asc: bool = True,
) -> list[dict[str, str]]:
    """
    Fetches the list of courses for a given subject and term from SIS.

    The term and subject search state on the SIS server must be reset before each call
    to this function.
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
        return data


async def get_course_details(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getClassDetails"
    )


async def get_course_description(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"


async def get_course_attributes(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionAttributes"


async def get_course_restrictions(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getRestrictions"
    )


async def get_course_corequisites(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCorequisites"
    )


async def get_course_prerequisites(session: aiohttp.ClientSession, term: str, crn: str):
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionPrerequisites"


async def get_course_crosslists(session: aiohttp.ClientSession, term: str, crn: str):
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getXlstSections"
    )


async def main():
    """
    A JSESSIONID cookie is required before accessing any course data, which can be
    obtained on the first request to any SIS page. The cookie should automatically be
    included in subsequent requests made with the same aiohttp session.

    reset_course_search() must be called before each new attempt to fetch courses from
    a different subject.
    """
    async with aiohttp.ClientSession(
        # headers={
        #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        #     "Accept-Encoding": "gzip, deflate, br, zstd",
        #     "Accept-Language": "en-US,en;q=0.9",
        #     "Connection": "keep-alive",
        #     "Host": "sis9.rpi.edu",
        #     "Sec-Ch-Ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Microsoft Edge";v="140"',
        #     "Sec-Ch-Ua-Mobile": "?0",
        #     "Sec-Ch-Ua-Platform": '"macOS"',
        #     "Sec-Fetch-Dest": "document",
        #     "Sec-Fetch-Mode": "navigate",
        #     "Sec-Fetch-Site": "none",
        #     "Sec-Fetch-User": "?1",
        #     "Upgrade-Insecure-Requests": "1",
        # }
    ) as session:
        term = "202409"
        subjects = await get_subjects(session, term)
        await reset_course_search(session, term)
        data = await course_search(session, term, subjects[0]["code"])
        # print(json.dumps(data, indent=4))
    return True


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Total time elapsed: {end_time - start_time:.2f} seconds")
