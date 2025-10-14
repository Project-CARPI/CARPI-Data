import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp

from app import logger

from .sis_api import *

_OUTPUT_DATA_DIR = Path(__file__).parent / "scraper_data"
_CODE_MAPPINGS_DIR = Path(__file__).parent / "code_mappings"
_SUBJ_NAME_CODE_MAP_PATH = _CODE_MAPPINGS_DIR / "subject_name_code_map.json"
_KNOWN_INSTRUCTOR_RCSIDS_PATH = _CODE_MAPPINGS_DIR / "known_instructor_rcsids.json"
_RESTRICTION_NAME_CODE_MAP_PATH = _CODE_MAPPINGS_DIR / "restriction_name_code_map.json"
_ATTRIBUTE_NAME_CODE_MAP_PATH = _CODE_MAPPINGS_DIR / "attribute_name_code_map.json"
_IS_RUNNING = False


def is_running() -> bool:
    """
    Returns whether the scraper is currently running.
    """
    return _IS_RUNNING


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


async def process_class_details(
    session: aiohttp.ClientSession,
    course_data: dict[str, Any],
    class_entry: dict[str, Any],
    known_rcsid_set: set[str] = None,
    attribute_name_code_map: dict[str, str] = None,
    restriction_name_code_map: dict[str, str] = None,
) -> None:
    """
    Fetches and parses all details for a given class, populating the provided course
    data dictionary or adding to existing entries as appropriate.

    Accepts optional sets and maps to populate with known RCSIDs, attribute names to codes,
    restriction names to codes, and subject names to codes. These would be used for
    post-processing and codification of the scraped data.

    Takes as input class data fetched from SIS's class search endpoint.
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
                get_class_prerequisites(session, term, crn)
            )
            corequisites_task = tg.create_task(
                get_class_corequisites(session, term, crn)
            )
            crosslists_task = tg.create_task(get_class_crosslists(session, term, crn))

        # Wait for tasks to complete and get results
        description_data = description_task.result()
        attributes_data = attributes_task.result()
        restrictions_data = restrictions_task.result()
        prerequisites_data = prerequisites_task.result()
        corequisites_data = corequisites_task.result()
        crosslists_data = crosslists_task.result()

        # Build attribute name to code map
        # Attributes are known to be in the format "Attribute Name  CODE"
        # Note the double space between name and code
        if attribute_name_code_map is not None:
            for attribute in attributes_data:
                attribute_split = attribute.split()
                if len(attribute_split) < 2:
                    logger.warning(
                        f"Unexpected attribute format for CRN {crn} in term {term}: {attribute}"
                    )
                    continue
                attribute_code = attribute_split[-1].strip()
                attribute_name = " ".join(attribute_split[:-1]).strip()
                if (
                    attribute_name in attribute_name_code_map
                    and attribute_name_code_map[attribute_name] != attribute_code
                ):
                    logger.warning(
                        f"Conflicting attribute codes for {attribute_name} in term {term}: {attribute_name_code_map[attribute_name]} vs. {attribute_code}"
                    )
                attribute_name_code_map[attribute_name] = attribute_code

        # Build restriction name to code map
        # Restrictions are known to be in the format "Restriction Name (CODE)"
        # Note the parentheses around the code
        if restriction_name_code_map is not None:
            restriction_pattern = r"(.*)\((.*)\)"
            for restriction_type in restrictions_data:
                for restriction in restrictions_data[restriction_type]:
                    restriction_match = re.match(restriction_pattern, restriction)
                    if restriction_match is None or len(restriction_match.groups()) < 2:
                        logger.warning(
                            f"Unexpected restriction format for CRN {crn} in term {term}: {restriction}"
                        )
                        continue
                    restriction_name = restriction_match.group(1).strip()
                    restriction_code = restriction_match.group(2).strip()
                    if (
                        restriction_name in restriction_name_code_map
                        and restriction_name_code_map[restriction_name]
                        != restriction_code
                    ):
                        logger.warning(
                            f"Conflicting restriction codes for {restriction_name} in term {term}: {restriction_name_code_map[restriction_name]} vs. {restriction_code}"
                        )
                    restriction_name_code_map[restriction_name] = restriction_code

        # Example course code: CSCI 1100
        course_data[course_code] = {
            "course_name": class_entry["courseTitle"],
            "course_detail": {
                "description": description_data,
                "corequisite": corequisites_data,
                "prerequisite": prerequisites_data,
                "crosslist": crosslists_data,
                "attributes": attributes_data,
                "restrictions": restrictions_data,
                "credits": {
                    "min": float("inf"),
                    "max": 0,
                },
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
        if "emailAddress" in faculty:
            email_address = faculty["emailAddress"]
            if email_address is not None and email_address.endswith("@rpi.edu"):
                rcsid = email_address.replace("@rpi.edu", "")
                # Add to known RCSID set if provided
                if known_rcsid_set is not None:
                    known_rcsid_set.add(rcsid)
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
    known_rcsid_set: set[str] = None,
    restriction_name_code_map: dict[str, str] = None,
    attribute_name_code_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(1),
    limit_per_host: int = 5,
    timeout: int = 60,
) -> dict[str, dict[str, Any]]:
    """
    Gets all course data for a given term and subject.

    Accepts optional sets and maps to populate with known RCSIDs, attribute names to codes,
    restriction names to codes, and subject names to codes. These would be used for
    post-processing and codification of the scraped data.

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
        timeout = aiohttp.ClientTimeout(total=timeout)

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
                                session,
                                course_data,
                                class_entry,
                                known_rcsid_set=known_rcsid_set,
                                restriction_name_code_map=restriction_name_code_map,
                                attribute_name_code_map=attribute_name_code_map,
                            )
                        )
                # Return data sorted by course code
                return dict(sorted(course_data.items()))
            except aiohttp.ClientError as e:
                logger.error(f"Error processing subject {subject} in term {term}: {e}")
                return {}


async def get_term_course_data(
    term: str,
    output_path: Path | str = None,
    subject_name_code_map: dict[str, str] = None,
    known_rcsid_set: set[str] = None,
    restriction_name_code_map: dict[str, str] = None,
    attribute_name_code_map: dict[str, str] = None,
    semaphore: asyncio.Semaphore = asyncio.Semaphore(10),
    limit_per_host: int = 5,
    timeout: int = 60,
) -> None:
    """
    Gets all course data for a given term, which includes all subjects in the term.

    Accepts optional sets and maps to populate with known RCSIDs, attribute names to codes,
    restriction names to codes, and subject names to codes. These would be used for
    post-processing and codification of the scraped data.

    This function spawns a client session for each subject to be processed in the term.
    A semaphore is used to limit the number of concurrent sessions, and an additional
    limit is placed on the number of simultaneous connections a session can make to the
    SIS server.

    Writes data as JSON after all subjects in the term have been processed.
    """
    async with aiohttp.ClientSession() as session:
        subjects = await get_term_subjects(session, term)

    # Build subject name to code map
    if subject_name_code_map is not None:
        for subject in subjects:
            if (
                subject["description"] in subject_name_code_map
                and subject_name_code_map[subject["description"]] != subject["code"]
            ):
                logger.warning(
                    f"Conflicting subject codes for {subject['description']} in term {term}: {subject_name_code_map[subject['description']]} vs. {subject['code']}"
                )
            subject_name_code_map[subject["description"]] = subject["code"]
    logger.info(f"Processing {len(subjects)} subjects for term: {term}")

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
                    term,
                    subject_code,
                    known_rcsid_set=known_rcsid_set,
                    restriction_name_code_map=restriction_name_code_map,
                    attribute_name_code_map=attribute_name_code_map,
                    semaphore=semaphore,
                    limit_per_host=limit_per_host,
                    timeout=timeout,
                )
            )
            tasks.append(task)

    # Wait for all tasks to complete and gather results
    for i, subject in enumerate(subjects):
        course_data = tasks[i].result()
        all_course_data[subject["code"]]["courses"] = course_data

    if len(all_course_data) == 0:
        return

    # Write all data for term to JSON file
    if output_path is None:
        output_path = _OUTPUT_DATA_DIR / f"{term}.json"
    elif isinstance(output_path, str):
        output_path = Path(output_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Writing data to {output_path}")
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(all_course_data, f, indent=4, ensure_ascii=False)


async def main(
    start_year: int = 1998,
    end_year: int = datetime.now().year,
    seasons: list[str] = None,
    semaphore_val: int = 10,
    limit_per_host: int = 5,
    timeout: int = 120,
) -> bool:
    """
    Runs the SIS scraper for the specified range of years and seasons.

    Seasons can be any combination of "spring", "summer", and "fall". If not specified,
    all three seasons will be processed by default.

    Spawns multiple client sessions to process subjects in parallel, which can optionally
    be limited by a semaphore. An additional limit can be placed on the number of
    simultaneous connections a session can make to the SIS server, as well as a timeout
    for all requests made by a session.

    Returns True on success or False on any unhandled failure.
    """

    # A JSESSIONID cookie is required before accessing any course data, which can be
    # obtained on the first request to any SIS page. The cookie should automatically be
    # included in subsequent requests made with the same aiohttp session.
    #
    # The term and subject search state on the SIS server must be reset before each attempt
    # to fetch classes from a term and subject.

    global _IS_RUNNING
    if _IS_RUNNING:
        logger.warning("Scraper run requested but scraper is already running")
        return False
    _IS_RUNNING = True

    start_time = time.time()

    if seasons is None:
        seasons = ["spring", "summer", "fall"]

    # Create name to code maps for codifying scraped data in post-processing
    subject_name_code_map = {}
    known_rcsid_set = set()
    restriction_name_code_map = {}
    attribute_name_code_map = {}

    try:
        # Load code maps for codifying scraped data in post-processing
        logger.info(
            f"Attempting to load existing subject code mappings from {_SUBJ_NAME_CODE_MAP_PATH}"
        )
        if _SUBJ_NAME_CODE_MAP_PATH.exists():
            with _SUBJ_NAME_CODE_MAP_PATH.open("r", encoding="utf-8") as f:
                subject_name_code_map = json.load(f)
            logger.info(f"  Loaded {len(subject_name_code_map)} subject code mappings")
        else:
            logger.info("  No existing subject code mappings found")

        logger.info(
            f"Attempting to load existing known instructor RCSIDs from {_KNOWN_INSTRUCTOR_RCSIDS_PATH}"
        )
        if _KNOWN_INSTRUCTOR_RCSIDS_PATH.exists():
            with _KNOWN_INSTRUCTOR_RCSIDS_PATH.open("r", encoding="utf-8") as f:
                known_rcsid_list = json.load(f)
                known_rcsid_set = set(known_rcsid_list)
            logger.info(f"  Loaded {len(known_rcsid_set)} known instructor RCSIDs")
        else:
            logger.info("  No existing known instructor RCSIDs found")

        logger.info(
            f"Attempting to load existing restriction code mappings from {_RESTRICTION_NAME_CODE_MAP_PATH}"
        )
        if _RESTRICTION_NAME_CODE_MAP_PATH.exists():
            with _RESTRICTION_NAME_CODE_MAP_PATH.open("r", encoding="utf-8") as f:
                restriction_name_code_map = json.load(f)
            logger.info(
                f"  Loaded {len(restriction_name_code_map)} restriction code mappings"
            )
        else:
            logger.info("  No existing restriction code mappings found")

        logger.info(
            f"Attempting to load existing attribute code mappings from {_ATTRIBUTE_NAME_CODE_MAP_PATH}"
        )
        if _ATTRIBUTE_NAME_CODE_MAP_PATH.exists():
            with _ATTRIBUTE_NAME_CODE_MAP_PATH.open("r", encoding="utf-8") as f:
                attribute_name_code_map = json.load(f)
            logger.info(
                f"  Loaded {len(attribute_name_code_map)} attribute code mappings"
            )
        else:
            logger.info("  No existing attribute code mappings found")

        # Limit concurrent client sessions and simultaneous connections
        semaphore = asyncio.Semaphore(semaphore_val)

        logger.info("Starting SIS scraper with settings:")
        logger.info(f"  Years: {start_year} - {end_year}")
        logger.info(
            f"  Seasons: {', '.join(season.capitalize() for season in seasons)}"
        )
        logger.info(f"  Max concurrent sessions: {semaphore._value}")
        logger.info(f"  Max concurrent connections per session: {limit_per_host}")

        # Process terms in parallel
        async with asyncio.TaskGroup() as tg:
            for year in range(start_year, end_year + 1):
                for season in seasons:
                    term = get_term_code(year, season)
                    if term == "":
                        continue
                    output_path = Path(_OUTPUT_DATA_DIR) / f"{term}.json"
                    tg.create_task(
                        get_term_course_data(
                            term,
                            output_path=output_path,
                            subject_name_code_map=subject_name_code_map,
                            known_rcsid_set=known_rcsid_set,
                            restriction_name_code_map=restriction_name_code_map,
                            attribute_name_code_map=attribute_name_code_map,
                            semaphore=semaphore,
                            limit_per_host=limit_per_host,
                            timeout=timeout,
                        )
                    )

        _CODE_MAPPINGS_DIR.mkdir(parents=True, exist_ok=True)
        # Ensure code maps are sorted by key before writing
        subject_name_code_map = dict(sorted(subject_name_code_map.items()))
        restriction_name_code_map = dict(sorted(restriction_name_code_map.items()))
        attribute_name_code_map = dict(sorted(attribute_name_code_map.items()))

        logger.info(
            f"Writing {len(subject_name_code_map)} subject code mappings to {_SUBJ_NAME_CODE_MAP_PATH}"
        )
        with _SUBJ_NAME_CODE_MAP_PATH.open("w", encoding="utf-8") as f:
            json.dump(subject_name_code_map, f, indent=4, ensure_ascii=False)

        logger.info(
            f"Writing {len(known_rcsid_set)} known instructor RCSIDs to {_KNOWN_INSTRUCTOR_RCSIDS_PATH}"
        )
        with _KNOWN_INSTRUCTOR_RCSIDS_PATH.open("w", encoding="utf-8") as f:
            json.dump(sorted(list(known_rcsid_set)), f, indent=4, ensure_ascii=False)

        logger.info(
            f"Writing {len(restriction_name_code_map)} restriction code mappings to {_RESTRICTION_NAME_CODE_MAP_PATH}"
        )
        with _RESTRICTION_NAME_CODE_MAP_PATH.open("w", encoding="utf-8") as f:
            json.dump(restriction_name_code_map, f, indent=4, ensure_ascii=False)

        logger.info(
            f"Writing {len(attribute_name_code_map)} attribute code mappings to {_ATTRIBUTE_NAME_CODE_MAP_PATH}"
        )
        with _ATTRIBUTE_NAME_CODE_MAP_PATH.open("w", encoding="utf-8") as f:
            json.dump(attribute_name_code_map, f, indent=4, ensure_ascii=False)

    except Exception as e:
        logger.error(f"Error in main: {e}")
        import traceback

        traceback.print_exc()
        _IS_RUNNING = False
        return False

    end_time = time.time()
    logger.info("SIS scraper completed")
    logger.info(f"  Time elapsed: {end_time - start_time:.2f} seconds")

    _IS_RUNNING = False
    return True


if __name__ == "__main__":
    start_year = 2025
    end_year = 2025
    asyncio.run(main(start_year, end_year))
