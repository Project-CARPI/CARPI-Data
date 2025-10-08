import html
import json
import re
from enum import Enum
from typing import Any

import aiohttp
import bs4
from prereq_parser import parse_prereq

RESTRICTION_TYPE_MAP = {
    "Majors": "major",
    "Classes": "classification",
    "Levels": "level",
    "Degrees": "degree",
    "Campuses": "campus",
}


class ClassColumn(str, Enum):
    COURSE_TITLE = "courseTitle"
    SUBJECT_DESCRIPTION = "subjectDescription"
    COURSE_NUMBER = "courseNumber"
    SECTION = "sequenceNumber"
    CRN = "courseReferenceNumber"
    TERM = "term"


def html_unescape(obj: Any) -> Any:
    """
    Recursively unescape HTML entities in all string values within a complex
    structure (dicts, lists, tuples, sets). Dict keys are unescaped too.
    """
    if isinstance(obj, str):
        return html.unescape(obj)
    if isinstance(obj, dict):
        return {html_unescape(k): html_unescape(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [html_unescape(i) for i in obj]
    if isinstance(obj, tuple):
        return tuple(html_unescape(i) for i in obj)
    if isinstance(obj, set):
        return {html_unescape(i) for i in obj}
    return obj


async def get_term_subjects(
    session: aiohttp.ClientSession, term: str
) -> list[dict[str, str]]:
    """
    Fetches the list of subjects and codes for a given term from SIS. If the term is
    invalid or doesn't exist, returns an empty list.

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
    params = {"term": term, "offset": 1, "max": 2147483647}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_term_instructors(
    session: aiohttp.ClientSession, term: str
) -> list[dict[str, str]]:
    """
    Fetches the list of instructors for a given term from SIS. If the term is invalid
    or doesn't exist, returns an empty list.

    Returned data format is as follows:
    [
        {
            "code": "71297",
            "description": "Abbott, Claude"
        },
        ...
    ]
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/classSearch/get_instructor"
    params = {"term": term, "offset": 1, "max": 2147483647}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_all_attributes(
    session: aiohttp.ClientSession, search_term: str = ""
) -> list[dict[str, str]]:
    """
    Fetches the master list of attributes from SIS.

    Returned data format is as follows:
    [
        {
            "code": "COMM",
            "description": "Communication Intensive"
        },
        ...
    ]
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/classSearch/get_attribute"
    params = {"searchTerm": search_term, "offset": 1, "max": 2147483647}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_all_colleges(
    session: aiohttp.ClientSession, search_term: str = ""
) -> list[dict[str, str]]:
    """
    Fetches the master list of colleges (schools) and codes from SIS. Not to be confused
    with campuses.

    Returned data format is as follows:
    [
        {
            "code": "S",
            "description": "School of Science"
        },
        ...
    ]
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/classSearch/get_college"
    params = {"searchTerm": search_term, "offset": 1, "max": 2147483647}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    data = json.loads(raw_data)
    data = html_unescape(data)
    return data


async def get_all_campuses(
    session: aiohttp.ClientSession, search_term: str = ""
) -> list[dict[str, str]]:
    """
    Fetches the master list of campuses and codes from SIS. Not to be confused with
    colleges (schools).

    Returned data format is as follows:
    [
        {
            "code": "T",
            "description": "Troy"
        },
        ...
    ]
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/classSearch/get_campus"
    params = {"searchTerm": search_term}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    data = json.loads(raw_data)
    data = html_unescape(data)
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
    max_size: int = 2147483647,
    sort_column: ClassColumn = ClassColumn.SUBJECT_DESCRIPTION,
    sort_asc: bool = True,
) -> list[dict[str, Any]]:
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
    data = json.loads(raw_data)
    data = html_unescape(data)
    course_data = data["data"]
    if course_data is None:
        return []
    return course_data


async def get_class_description(
    session: aiohttp.ClientSession, term: str, crn: str
) -> str:
    """
    Fetches and parses data from the "Course Description" tab of a class details page.

    Returns a string containing the course description, without any additional fields
    such as "When Offered", "Credit Hours", "Prerequisite", etc.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    description_tag = soup.find("section", {"aria-labelledby": "courseDescription"})
    if description_tag is None:
        print(f"No description found for term and CRN: {term} - {crn}")
        return ""
    description_text_list = [
        text.strip() for text in description_tag.get_text(separator="\n").split("\n")
    ]
    for text in description_text_list:
        if text != "":
            return text


async def get_class_attributes(session: aiohttp.ClientSession, term: str, crn: str):
    """
    Fetches and parses data from the "Attributes" tab of a class details page.

    Returned data format is as follows:
    [
        "Attribute 1",
        "Attribute 2",
        "Attribute 3",
        ...
    ]
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionAttributes"
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    attributes = []
    attribute_tags = soup.find_all("span", {"class": "attribute-text"})
    for tag in attribute_tags:
        attributes.append(tag.text.strip())
    return attributes


async def get_class_restrictions(session: aiohttp.ClientSession, term: str, crn: str):
    """
    Fetches and parses data from the "Restrictions" tab of a class details page.

    Returned data format is as follows:
    {
        "major": ["Allowed Major 1", ...],
        "not_major": ["Disallowed Major 1", ...],
        "level": ["Allowed Level 1", ...],
        "not_level": ["Disallowed Level 1", ...],
        "classification": ["Allowed Classification 1", ...],
        "not_classification": ["Disallowed Classification 1", ...]
    }
    """
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getRestrictions"
    )
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    restrictions_data = {
        "major": [],
        "not_major": [],
        "level": [],
        "not_level": [],
        "classification": [],
        "not_classification": [],
        "degree": [],
        "not_degree": [],
        "campus": [],
        "not_campus": [],
    }
    restrictions_tag = soup.find("section", {"aria-labelledby": "restrictions"})
    # Other known restriction header patterns include:
    # "Special Approvals:"
    restriction_header_pattern = r"(Must|Cannot) be enrolled in one of the following (Majors|Classes|Levels|Degrees|Campuses):"
    # All known children of the restrictions section are <div>, <span<>, or <br> tags
    # Tags relevant to restrictions are only known to be <span> tags
    restrictions_content = [
        child
        for child in restrictions_tag.children
        if isinstance(child, bs4.element.Tag) and child.name == "span"
    ]
    i = 0
    while i < len(restrictions_content):
        content = restrictions_content[i]
        if content.string is None:
            print(
                f"Skipping unexpected restriction content with no string for term and CRN: {term} - {crn}"
            )
            i += 1
            continue
        content_string = content.string.strip()
        header_match = re.match(restriction_header_pattern, content_string)
        if header_match is None:
            i += 1
            continue
        must_or_cannot, type_plural = header_match.groups()
        key_base = RESTRICTION_TYPE_MAP[type_plural]
        key = f"not_{key_base}" if must_or_cannot == "Cannot" else key_base
        restriction_list = restrictions_data[key]
        i += 1
        while i < len(restrictions_content):
            next_content = restrictions_content[i]
            if next_content.string is None:
                print(
                    f"Skipping unexpected restriction content with no string for term and CRN: {term} - {crn}"
                )
                i += 1
                continue
            next_content_string = next_content.string.strip()
            # Stop if another restriction header is encountered
            if re.match(restriction_header_pattern, next_content_string):
                break
            restriction_list.append(next_content_string)
            i += 1
    return restrictions_data


async def get_class_prerequisites(
    session: aiohttp.ClientSession,
    term: str,
    crn: str,
    subject_name_code_map: dict[str, str],
) -> dict[str, Any]:
    """
    Fetches and parses data from the "Prerequisites" tab of a class details page.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getSectionPrerequisites"
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        text = await response.text()
    text = html.unescape(text)
    soup = bs4.BeautifulSoup(text, "html5lib")
    data = ""
    rows = soup.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        if len(cols) == 0:
            continue
        data += (
            " and " if cols[0].text == "And" else " or " if cols[0].text == "Or" else ""
        )
        data += " ( " if cols[1].text != "" else ""
        if cols[2].text != "":
            data += f" {cols[2].text} {cols[3].text} "
        else:
            if cols[4].text not in subject_name_code_map:
                print(f"Unknown department in CRN {crn}: {cols[4].text}")
                data += f" {cols[4].text} {cols[5].text} "
            else:
                data += f" {subject_name_code_map[cols[4].text]} {cols[5].text} "
        data += " ) " if cols[8].text != "" else ""
        data = data.replace("  ", " ").strip()
        data = data.replace("  ", " ").strip()
        data = data.replace("( ", "(").strip()
        data = data.replace(" )", ")").strip()
    if data:
        try:
            return parse_prereq(crn, data)
        except Exception as e:
            print(f"Error parsing prerequisites for CRN {crn} with data: {data} - {e}")
    return {}


async def get_class_corequisites(
    session: aiohttp.ClientSession,
    term: str,
    crn: str,
    subject_name_code_map: dict[str, str] = None,
):
    """
    Fetches and parses data from the "Corequisites" tab of a class details page.

    Accepts an optional subject name to subject code mapping. If provided, subject
    names will be attempted to be converted to subject codes in the returned data,
    e.g. "Biology" -> "BIOL".

    Returned data format is as follows:
    [
        "Computer Science 1100",
        "Mathematics 1010",
        ...
    ]
    """
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getCorequisites"
    )
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    coreqs_tag = soup.find("section", {"aria-labelledby": "coReqs"})
    coreqs_table = coreqs_tag.find("table", {"class": "basePreqTable"})
    if coreqs_table is None:
        return []
    coreqs_thead = coreqs_table.thead
    coreqs_tbody = coreqs_table.tbody
    if not coreqs_thead or not coreqs_tbody:
        return []
    thead_cols = [th.text.strip() for th in coreqs_thead.find_all("th")]
    # Known corequisite columns are Subject, Course, and Title
    if len(thead_cols) != 3:
        print(
            f"Unexpected number of corequisite columns for term and CRN: {term} - {crn}"
        )
        return []
    # Corequisite list should be a list of course codes
    # e.g. "CSCI 1100", "MATH 1010"
    coreqs = []
    for tr in coreqs_tbody.find_all("tr"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if len(cols) != len(thead_cols):
            print(
                f"Skipping unexpected corequisite row with mismatched columns for term and CRN: {term} - {crn}"
            )
            continue
        subject = cols[0]
        # Convert subject name to code if mapping is provided
        if subject_name_code_map and subject in subject_name_code_map:
            subject = subject_name_code_map[subject]
        course_num = cols[1]
        coreqs.append(f"{subject} {course_num}")
    return coreqs


async def get_class_crosslists(
    session: aiohttp.ClientSession,
    term: str,
    crn: str,
    subject_name_code_map: dict[str, str] = None,
):
    """
    Fetches and parses data from the "Cross Listed" tab of a class details page.

    Accepts an optional subject name to subject code mapping. If provided, subject
    names will be attempted to be converted to subject codes in the returned data,
    e.g. "Biology" -> "BIOL".

    Returned data format is as follows:
    [
        "Computer Science 1100",
        "Mathematics 1010",
        ...
    ]
    """
    url = (
        "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/getXlstSections"
    )
    params = {"term": term, "courseReferenceNumber": crn}
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        raw_data = await response.text()
    raw_data = html_unescape(raw_data)
    soup = bs4.BeautifulSoup(raw_data, "html5lib")
    crosslists_tag = soup.find("section", {"aria-labelledby": "xlstSections"})
    crosslists_table = crosslists_tag.table
    if crosslists_table is None:
        return []
    crosslists_thead = crosslists_table.thead
    crosslists_tbody = crosslists_table.tbody
    if not crosslists_thead or not crosslists_tbody:
        return []
    thead_cols = [th.text.strip() for th in crosslists_thead.find_all("th")]
    # Known crosslist columns are CRN, Subject, Course Number, Title, and Section
    if len(thead_cols) != 5:
        print(
            f"Unexpected number of crosslist columns for term and CRN: {term} - {crn}"
        )
        return []
    crosslists = []
    for tr in crosslists_tbody.find_all("tr"):
        cols = [td.text.strip() for td in tr.find_all("td")]
        if len(cols) != len(thead_cols):
            print(
                f"Skipping unexpected crosslist row with mismatched columns for term and CRN: {term} - {crn}"
            )
            continue
        subject = cols[1]
        code = cols[2]
        # Convert subject name to code if mapping is provided
        if subject_name_code_map and subject in subject_name_code_map:
            subject = subject_name_code_map[subject]
        crosslists.append(f"{subject} {code}")
    return crosslists
