import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
from sis_api import *

OUTPUT_DATA_DIR = "data"


async def get_reverse_subject_map(
    session: aiohttp.ClientSession,
    start_year: int = 1998,
    end_year: int = datetime.now().year,
    seasons: list[str] = None,
) -> dict[str, str]:
    """
    Fetches the list of subjects from the specified range of years and seasons, and
    returns a "reverse" mapping of subject names to subject codes.

    Defaults to a range from 1998 to the current year, and Spring, Summer, and Fall
    seasons. SIS data begins in Summer 1998.

    Returned data format is as follows:
    {
        "Biology": "BIOL",
        "Computer Science": "CSCI",
        ...
    }
    """
    subject_name_code_map = {}
    if seasons is None:
        seasons = ["spring", "summer", "fall"]
    for year in range(start_year, end_year + 1):
        for season in seasons:
            term = get_term_code(year, season)
            subjects = await get_term_subjects(session, term)
            for subject in subjects:
                subject_name_code_map[subject["description"]] = subject["code"]
    return subject_name_code_map


async def process_class_details(
    session: aiohttp.ClientSession,
    course_data: dict[str, Any],
    class_entry: dict[str, Any],
    subject_name_code_map: dict[str, str] = None,
) -> None:
    """
    Fetches and parses all details for a given class, populating the provided course
    data dictionary or adding to existing entries as appropriate.

    Takes as input class data fetched from SIS's class search endpoint.

    Accepts an optional subject name to subject code mapping. If provided, subject
    names will be attempted to be converted to subject codes in the returned data,
    e.g. "Biology" -> "BIOL".
    """
    # Example course code: CSCI 1100
    course_code = f"{class_entry['subject']} {class_entry['courseNumber']}"

    # Fetch class details not included in main class details
    # Only fetch if course not already in course data
    if course_code not in course_data:
        term = class_entry["term"]
        crn = class_entry["courseReferenceNumber"]
        async with asyncio.TaskGroup() as tg:
            description_task = tg.create_task(get_class_description(session, term, crn))
            attributes_task = tg.create_task(get_class_attributes(session, term, crn))
            restrictions_task = tg.create_task(
                get_class_restrictions(session, term, crn)
            )
            prerequisites_task = tg.create_task(
                get_class_prerequisites(session, term, crn, subject_name_code_map)
            )
            corequisites_task = tg.create_task(
                get_class_corequisites(session, term, crn, subject_name_code_map)
            )
            crosslists_task = tg.create_task(
                get_class_crosslists(session, term, crn, subject_name_code_map)
            )

        # Wait for tasks to complete and get results
        description_data = description_task.result()
        attributes_data = attributes_task.result()
        restrictions_data = restrictions_task.result()
        prerequisites_data = prerequisites_task.result()
        corequisites_data = corequisites_task.result()
        crosslists_data = crosslists_task.result()

        # Example course code: CSCI 1100
        course_data[course_code] = {
            "course_name": class_entry["courseTitle"],
            "course_detail": {
                "description": description_data["description"],
                "corequisite": corequisites_data,
                "prerequisite": prerequisites_data,
                "crosslist": crosslists_data,
                "attributes": attributes_data,
                "restrictions": restrictions_data,
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
    class_faculty = class_entry["faculty"]
    class_faculty_rcsids = []
    for faculty in class_faculty:
        rcsid = f"Unknown RCSID ({faculty['displayName']})"
        if "emailAddress" in faculty and faculty["emailAddress"] is not None:
            rcsid = faculty["emailAddress"].replace("@rpi.edu", "")
        class_faculty_rcsids.append(rcsid)

    course_sections.append(
        {
            "CRN": class_entry["courseReferenceNumber"],
            "instructor": class_faculty_rcsids,
            "schedule": {},
            "capacity": class_entry["maximumEnrollment"],
            "registered": class_entry["enrollment"],
            "open": class_entry["seatsAvailable"],
        }
    )


async def get_course_data(
    term: str,
    subject: str,
    subject_name_code_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1),
    limit_per_host: int = 5,
) -> dict[str, dict[str, Any]]:
    """
    Gets all course data for a given term and subject.

    Accepts an optional subject name to subject code mapping. If provided, subject
    names will be attempted to be converted to subject codes in the returned data,
    e.g. "Biology" -> "BIOL".

    This function spawns its own client session to avoid session state conflicts with
    other subjects that may be processing concurrently. Optionally accepts a semaphore
    to limit the number of concurrent sessions between multiple calls to this function,
    as well as a limit on the number of simultaneous connections a session can make to
    the SIS server.

    In the context of this scraper, a "class" refers to a section of a course, while a
    "course" refers to the overarching course that may have multiple classes.

    The data returned from SIS is keyed by classes, not courses. This function
    manipulates and aggregates this data such that the returned structure is keyed by
    courses instead, with classes as a sub-field of each course.
    """

    async with semaphore:
        # Limit simultaneous connections to SIS server per session
        connector = aiohttp.TCPConnector(limit_per_host=limit_per_host)
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
                    for class_entry in class_data:
                        tg.create_task(
                            process_class_details(
                                session, course_data, class_entry, subject_name_code_map
                            )
                        )
                # Return data sorted by course code
                return dict(sorted(course_data.items()))
            except aiohttp.ClientError as e:
                print(f"Error processing subject {subject} in term {term}: {e}")
                return {}


async def get_term_course_data(
    term: str,
    output_path: Path | str = None,
    subject_name_code_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    limit_per_host: int = 5,
) -> dict[str, dict[str, Any]]:
    """
    Gets all course data for a given term, which includes all subjects in the term.

    This function spawns a client session for each subject to be processed in the term.
    A semaphore is used to limit the number of concurrent sessions, and an additional
    limit is placed on the number of simultaneous connections a session can make to the
    SIS server.

    Accepts an optional subject name to subject code mapping. If provided, subject
    names will be attempted to be converted to subject codes in the returned data,
    e.g. "Biology" -> "BIOL".

    Writes data as JSON after all subjects in the term have been processed.
    """
    print(f"Fetching subject list for term: {term}")
    async with aiohttp.ClientSession() as session:
        subjects = await get_term_subjects(session, term)
    print(f"Processing {len(subjects)} subjects for term: {term}")

    # Stores all course data for the term
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
            task = tg.create_task(
                get_course_data(
                    term, subject_code, subject_name_code_map, semaphore, limit_per_host
                )
            )
            tasks.append(task)

    # Wait for all tasks to complete and gather results
    for i, subject in enumerate(subjects):
        course_data = tasks[i].result()
        all_course_data[subject["code"]]["courses"] = course_data

    # Write all data for term to JSON file
    if output_path is None:
        output_path = Path(f"data/{term}.json")
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Writing data to {output_path}")
    with output_path.open("w") as f:
        json.dump(all_course_data, f, indent=4, ensure_ascii=False)


def get_term_code(year: int, season: str) -> str:
    """
    Converts a year and academic season into a term code used by SIS.
    """
    if season is None:
        return ""
    season_lower = season.lower().strip()
    if season_lower == "fall":
        return f"{year}09"
    elif season_lower == "summer":
        return f"{year}05"
    elif season_lower == "spring":
        return f"{year}01"
    else:
        return ""


async def main(start_year: int, end_year: int, seasons: list[str] = None) -> bool:
    """
    Runs the SIS scraper for the specified range of years and seasons.

    Seasons can be any combination of "spring", "summer", and "fall". If not specified,
    all three seasons will be processed by default.

    Returns True on success or False on any unhandled failure.
    """

    # A JSESSIONID cookie is required before accessing any course data, which can be
    # obtained on the first request to any SIS page. The cookie should automatically be
    # included in subsequent requests made with the same aiohttp session.
    #
    # The term and subject search state on the SIS server must be reset before each attempt
    # to fetch classes from a term and subject.

    if seasons is None:
        seasons = ["spring", "summer", "fall"]

    try:
        # Limit concurrent client sessions and simultaneous connections
        semaphore = asyncio.Semaphore(50)
        limit_per_host = 20

        # Create master subject name to subject code mapping
        print("Fetching subject name to subject code mapping...")
        async with aiohttp.ClientSession() as session:
            subject_name_code_map = await get_reverse_subject_map(
                session, seasons=seasons
            )

        # Process terms in parallel
        async with asyncio.TaskGroup() as tg:
            for year in range(start_year, end_year + 1):
                for season in seasons:
                    term = get_term_code(year, season)
                    if term == "":
                        continue
                    output_path = Path(OUTPUT_DATA_DIR) / f"{term}.json"
                    tg.create_task(
                        get_term_course_data(
                            term,
                            output_path,
                            subject_name_code_map,
                            semaphore,
                            limit_per_host,
                        )
                    )

    except Exception as e:
        print(f"Error in main: {e}")
        import traceback

        traceback.print_exc()
        return False
    return True


if __name__ == "__main__":
    start_year = 2025
    end_year = 2025
    start_time = time.time()
    asyncio.run(main(start_year, end_year))
    end_time = time.time()
    print(f"Total time elapsed: {end_time - start_time:.2f} seconds")
