import asyncio
import json
import time

import aiohttp
import bs4


async def get_subjects(
    session: aiohttp.ClientSession, term: str
) -> list[dict[str, str]]:
    """
    Fetches the list of subjects for a given term from the SIS API.

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
    async with session.get(
        url, params={"term": term, "offset": 1, "max": 100}
    ) as response:
        response.raise_for_status()
        data = await response.json()
        return data


async def reset_subject_search(session: aiohttp.ClientSession, term: str) -> None:
    """
    Resets the subject search state on the SIS server.

    Must be re-called before each new attempt to fetch courses from a different subject.
    Otherwise, the server will continue returning results from the last subject accessed.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/term/search?mode=search"
    async with session.get(url, params={"term": term}) as response:
        response.raise_for_status()


async def get_courses_in_subject(
    session: aiohttp.ClientSession, term: str, subject: str
) -> list[dict[str, str]]:
    """
    Fetches the list of courses for a given subject and term from the SIS API.

    reset_subject_search() must be called once before each call to this function.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/searchResults/searchResults"
    params = {
        "txt_subject": subject,
        "txt_term": term,
        "pageOffset": 0,
        "pageMaxSize": 1000,
        "sortColumn": "subjectDescription",
        "sortDirection": "asc",
    }
    async with session.get(url, params=params) as response:
        response.raise_for_status()
        data = await response.json()
        return data


async def main():
    """
    A JSESSIONID cookie is required before accessing any course data, which can be
    obtained on the first request to any SIS page. The cookie should automatically be
    included in subsequent requests made with the same aiohttp session.

    reset_subject_search() must be called before each new attempt to fetch courses from
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
        await reset_subject_search(session, term)
        data = await get_courses_in_subject(session, term, subjects[0]["code"])
        print(json.dumps(data, indent=4))


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    print(f"Total time elapsed: {end_time - start_time:.2f} seconds")
