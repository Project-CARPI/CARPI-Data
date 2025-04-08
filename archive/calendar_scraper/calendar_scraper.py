import json

import requests
from bs4 import BeautifulSoup

if __name__ == "__main__":
    URL = "https://info.rpi.edu/registrar/academic-calendar"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                      "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0"
    }
    status_code = 0
    with requests.Session() as session:
        while status_code != 200:
            resp = session.get(
                url = URL,
                headers = HEADERS,
                timeout = 10
            )
            status_code = resp.status_code
            print(f"Response code: {status_code}")
    soup = BeautifulSoup(resp.content, "html5lib")
    calendar_div = soup.find("div", id="academicCalendar")
    assert calendar_div
    year = 1970
    year_data = dict()
    for month_table in calendar_div.find_all("table", class_="acadcal"):
        assert month_table.thead.text and month_table.tbody
        assert month_table.thead.text.count(" ") == 1
        year = month_table.thead.text.split()[1]
        month_name = month_table.thead.text.split()[0]
        year_data[month_name] = []
        for event in month_table.tbody.find_all("tr"):
            date_cell = event.find("td", class_="date")
            assert date_cell
            anchor = event.find("a")
            assert anchor
            assert anchor["href"]
            event_date = date_cell.text.strip()
            # Replaces unicode right single quotation mark
            event_name = anchor.text.replace("\u2019", "'")
            event_link = anchor["href"]
            year_data[month_name].append(
                {
                    "date": event_date,
                    "title": event_name,
                    "url": event_link
                }
            )
    with open(f"acadcal_{year}.json", "w") as outfile:
        json.dump(year_data, outfile, indent=4)