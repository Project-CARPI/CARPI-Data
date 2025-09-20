import asyncio
import json
import time

import aiohttp
import bs4

# from app import logger


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


async def get_cookie(session: aiohttp.ClientSession, term: str) -> str:
    """
    Fetches a JSESSIONID cookie from the SIS API.

    Must be re-called before each new request to get all courses in a subject.
    """
    url = "https://sis9.rpi.edu/StudentRegistrationSsb/ssb/term/search?mode=search"
    async with session.get(
        url,
        params={"term": term},
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Host": "sis9.rpi.edu",
            "Sec-Ch-Ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Microsoft Edge";v="140"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    ) as response:
        response.raise_for_status()
        cookies = response.cookies
        headers = response.headers
        print(cookies or "No cookies")
        print(headers)
        return cookies["JSESSIONID"].value


async def get_courses_in_subject(
    session: aiohttp.ClientSession, jsessionid_cookie: str, term: str, subject: str
) -> list[dict[str, str]]:
    """
    Fetches the list of courses for a given subject and term from the SIS API.

    Returned data format is as follows:
    [
        {
            "courseId": "12345",
            "subject": "CSE",
            "catalogNbr": "1010",
            "titleLong": "Introduction to Computer Science",
            ...
        },
        ...
    ]
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
    headers = {"Cookie": f"JSESSIONID={jsessionid_cookie}"}
    async with session.get(url, params=params, headers=headers) as response:
        response.raise_for_status()
        data = await response.json()
        return data


async def main():
    async with aiohttp.ClientSession(
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0",
            "Accept": "*/*",
        }
    ) as session:
        term = "202409"
        subjects = await get_subjects(session, term)
        cookie = await get_cookie(session, term)
        data = await get_courses_in_subject(session, cookie, term, subjects[0]["code"])
        print(json.dumps(data, indent=4))


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    # logger.info(f"Execution time: {end_time - start_time} seconds")
