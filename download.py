import logging
import pathlib
import random
import time

import requests
import bs4

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0)"
        " Gecko/20100101 Firefox/52.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}
_PMC_URL_TEMPLATE = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{}"
_PMC_TABLE_URL_TEMPLATE = f"{_PMC_URL_TEMPLATE}/table/{{}}/"


def _sleep():
    delay = random.uniform(5, 15)
    logging.debug(f"Sleep for {delay:.0f} seconds")
    time.sleep(delay)


def _get_article(
    pmcid: int, session: requests.Session, output_dir: pathlib.Path
) -> pathlib.Path:
    output_file = output_dir / f"article_{pmcid}.html"
    if output_file.is_file():
        return output_file
    url = _PMC_URL_TEMPLATE.format(pmcid)
    _sleep()
    response = session.get(url)
    response.raise_for_status()
    output_file.write_bytes(response.content)
    return output_file


def _get_table_ids(article_file: pathlib.Path) -> list[str]:
    html = bs4.BeautifulSoup(article_file.read_text("UTF-8"), "html5lib")
    table_wraps = html.find_all(class_="table-wrap")
    table_ids = [tw["id"] for tw in table_wraps]
    return table_ids


def _get_tables(
    pmcid: int,
    article_file: pathlib.Path,
    session: requests.Session,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    all_table_ids = _get_table_ids(article_file)
    tables_dir = output_dir / f"tables_{pmcid}"
    tables_dir.mkdir(exist_ok=True)
    for table_id in all_table_ids:
        table_file = tables_dir / f"{table_id}"
        if table_file.is_file():
            continue
        url = _PMC_TABLE_URL_TEMPLATE.format(pmcid, table_id)
        _sleep()
        response = session.get(url)
        response.raise_for_status()
        table_file.write_bytes(response.content)
    return tables_dir


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")

    output_dir = pathlib.Path("data", "articles")
    output_dir.mkdir(exist_ok=True, parents=True)

    with open("pmcids.txt") as stream:
        pmcids = list(map(int, stream))

    assert len(pmcids) == 230
    pmcids = pmcids[:2]

    with requests.Session() as session:
        session.headers.update(_HEADERS)
        html_file = _get_article(pmcids[0], session, output_dir)
    print(html_file)
    tables_dir = _get_tables(pmcids[0], html_file, session, output_dir)
    print(tables_dir)

    # - download html
    # - fix xhtml, parse
    # - find table ids
    # - download tables
