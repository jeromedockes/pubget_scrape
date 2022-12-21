import argparse
import hashlib
import json
import logging
import pathlib
import random
import time

import bs4
import pandas as pd
from pubget import _coordinates
import requests

_LOG_FORMAT = "%(levelname)s\t%(asctime)s\t%(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
_COORD_FIELDS = ("pmcid", "table_id", "x", "y", "z")
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
_PMC_TABLE_URL_TEMPLATE = f"{_PMC_URL_TEMPLATE}/table/{{}}/?report=objectonly"


def _sleep():
    delay = 2.0 + random.expovariate(1.0 / 8.0)
    logging.debug(f"Sleep for {delay:.0f} seconds")
    time.sleep(delay)


def _get_article(
    pmcid: int, session: requests.Session, output_dir: pathlib.Path
) -> pathlib.Path:
    output_file = output_dir / f"pmcid_{pmcid}_article.html"
    if output_file.is_file():
        return output_file
    logging.debug(f"Downloading text for PMCID {pmcid}")
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


def _short_hash(name: str) -> str:
    return hashlib.md5(name.encode("UTF-8")).hexdigest()[:6]


def _get_tables(
    pmcid: int,
    article_file: pathlib.Path,
    session: requests.Session,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    all_table_ids = _get_table_ids(article_file)
    logging.debug(f"Found {len(all_table_ids)} tables in PMCID {pmcid}")
    tables_dir = output_dir / f"pmcid_{pmcid}_tables"
    tables_dir.mkdir(exist_ok=True)
    id_info = {}
    for table_id in all_table_ids:
        table_name = _short_hash(table_id)
        id_info[table_name] = table_id
        table_file = tables_dir / f"{table_name}.html"
        if table_file.is_file():
            continue
        logging.debug(f"Downloading PMCID {pmcid} table {table_id}")
        url = _PMC_TABLE_URL_TEMPLATE.format(pmcid, table_id)
        _sleep()
        response = session.get(url)
        response.raise_for_status()
        table_file.write_bytes(response.content)
    id_info_file = tables_dir / "table_ids.json"
    id_info_file.write_text(json.dumps(id_info), "UTF-8")
    return tables_dir


def _get_coordinates(
    pmcid: int, tables_dir: pathlib.Path, output_dir: pathlib.Path
) -> pathlib.Path:
    all_table_ids = json.loads(
        (tables_dir / "table_ids.json").read_text("UTF-8")
    )
    all_coordinates = []
    for table_file in tables_dir.glob("*.html"):
        table_id = all_table_ids[table_file.stem]
        try:
            table = pd.read_html(table_file.read_text("UTF-8"))[0]
            coords = _coordinates._extract_coordinates_from_table(table)
        except Exception:
            continue
        coords["table_id"] = table_id
        coords["pmcid"] = pmcid
        all_coordinates.append(coords)
    if all_coordinates:
        result = pd.concat(all_coordinates).loc[:, _COORD_FIELDS]
    else:
        result = pd.DataFrame(columns=_COORD_FIELDS)
    logging.debug(f"Found {result.shape[0]} coordinates for PMCID {pmcid}")
    coordinates_file = output_dir / f"pmcid_{pmcid}_coordinates.csv"
    result.to_csv(coordinates_file, index=False)
    return coordinates_file


def _process_pmcid(
    pmcid: int, session: requests.Session, output_dir: pathlib.Path
) -> pathlib.Path:
    logging.info(f"Processing PMCID {pmcid}")
    html_file = _get_article(pmcid, session, output_dir)
    tables_dir = _get_tables(pmcid, html_file, session, output_dir)
    coords_file = _get_coordinates(pmcid, tables_dir, output_dir)
    return coords_file


def _process_all_pmcids(
    all_pmcids: list[int], output_dir: pathlib.Path
) -> pathlib.Path:
    output_dir.mkdir(exist_ok=True, parents=True)
    logging.info(f"Collecting data for {len(all_pmcids)} PMCIDs")
    logging.info(f"Storing results in '{output_dir}'")
    all_coords_files = []
    n_errors = 0
    with requests.Session() as session:
        session.headers.update(_HEADERS)
        for pmcid_idx, pmcid in enumerate(all_pmcids):
            try:
                all_coords_files.append(
                    _process_pmcid(pmcid, session, output_dir)
                )
            except Exception:
                n_errors += 1
                logging.exception(f"Failed to process PMCID {pmcid}")
            logging.info(
                f"Processed {pmcid_idx + 1} / {len(all_pmcids)} PMCIDs "
                f"({n_errors} errors)"
            )
    all_coords = []
    for coords_file in all_coords_files:
        all_coords.append(pd.read_csv(coords_file))
    merged_coords_file = output_dir / "all_coordinates.csv"
    merged_coords = pd.concat(all_coords)
    n_coords = merged_coords.shape[0]
    n_articles = len(pd.unique(merged_coords["pmcid"].values))
    n_successes = len(all_pmcids) - n_errors
    logging.info(
        f"Found {n_coords} coordinates from {n_articles} articles "
        f"({n_successes} PMCIDs successfully processed in total, "
        f"{n_errors} errors)"
    )
    merged_coords.to_csv(merged_coords_file, index=False)
    logging.info(f"Coordinates stored in {merged_coords_file}")
    return merged_coords_file


if __name__ == "__main__":
    logging.basicConfig(
        level="DEBUG", format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("pmcids_file", type=str)
    parser.add_argument("output_dir", type=str)
    args = parser.parse_args()

    with open(args.pmcids_file) as stream:
        pmcids = list(map(int, stream))

    output_dir = pathlib.Path(args.output_dir)
    coords_file = _process_all_pmcids(pmcids, output_dir)

    print(f"{coords_file}")
