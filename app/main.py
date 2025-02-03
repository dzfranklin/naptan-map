#!/usr/bin/env python

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import typing
import urllib.parse
from os import path
from typing import Optional
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup as Soup

# Reference: <https://naptan.dft.gov.uk/naptan/schema/2.4/doc/NaPTANSchemaGuide-2.4-v0.57.pdf>

naptan_api_csv_url = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"

attribution = "<a href=\"https://findtransportdata.dft.gov.uk/\" target=\"_blank\">DfT</a>" + \
              " via <a href=\"https://github.com/dzfranklin/naptan-map\">naptan-map</a>"

bunny_storage_key = os.getenv("BUNNY_STORAGE_KEY")
if bunny_storage_key is None:
    raise RuntimeError("Missing BUNNY_STORAGE_KEY")
bunny_key = os.getenv("BUNNY_KEY")
if bunny_key is None:
    raise RuntimeError("Missing BUNNY_KEY")


def main(scratch: str, naptan_csv_path: Optional[str] = None, dft_gtfs_path: Optional[str] = None):
    if naptan_csv_path is None:
        naptan_csv_path = naptan_api_csv_url
    print(f"Opening {naptan_csv_path}")
    if naptan_csv_path.startswith("https://"):
        naptan_csv_url = naptan_csv_path
        naptan_csv_path = path.join(scratch, "naptan.csv")
        with requests.get(naptan_csv_url, stream=True) as upload_r, open(naptan_csv_path, "wb+") as f:
            upload_r.raise_for_status()
            for chunk in upload_r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    if dft_gtfs_path is None:
        print("Downloading dft gtfs")

        username = os.getenv("DFT_BUS_DATA_USERNAME")
        if not username:
            raise RuntimeError("Missing DFT_BUS_DATA_USERNAME")
        password = os.getenv("DFT_BUS_DATA_PASSWORD")
        if not password:
            raise RuntimeError("Missing DFT_BUS_DATA_PASSWORD")

        dft_gtfs_path = path.join(scratch, "itm_all_gtfs.zip")
        download_dft_gtfs(username, password, dft_gtfs_path)

    print(f"Opening {dft_gtfs_path}")
    stops_in_gtfs: typing.Set[str] = set()
    with ZipFile(dft_gtfs_path) as zf:
        print(f"Reading {dft_gtfs_path}/stop_times.txt")
        f = zf.open("stop_times.txt")

        header = f.readline().decode().split(",")
        stop_id_i = header.index("stop_id")

        for line in f:
            fields = line.decode().split(",")
            stop_id = fields[stop_id_i]
            stops_in_gtfs.add(stop_id)
    print(f"Found {len(stops_in_gtfs)} stops with times in gtfs")

    naptan = pd.read_csv(
        naptan_csv_path,
        usecols=["ATCOCode", "NaptanCode", "CommonName", "ShortCommonName", "Indicator", "Longitude", "Latitude",
                 "BusStopType", "Status"],
        dtype={"ATCOCode": str, "NaptanCode": str, "CommonName": str, "ShortCommonName": str, "Indicator": str,
               "Longitude": float, "Latitude": float, "BusStopType": str, "Status": str}
    )

    # See schema guide, pages 95
    bus_stops = naptan.loc[naptan.BusStopType.isin(["MKD", "CUS"]) & (naptan.Status == "active")]

    bus_stops_geojson = path.join(scratch, "bus_stops_uk_geojson.json")
    bus_stops_prop_fields = ["ATCOCode", "CommonName", "ShortCommonName", "Indicator"]
    approx_row_count = len(stops_in_gtfs)
    skipped_stop_count = 0
    wrote_count = 0
    with open(bus_stops_geojson, "w+") as f:
        f.write('{"type": "FeatureCollection", "features": [')

        for i, row in enumerate(bus_stops.itertuples()):
            if row.ATCOCode not in stops_in_gtfs:
                skipped_stop_count += 1
                continue

            lng = row.Longitude
            lat = row.Latitude
            if pd.isna(lng) or pd.isna(lat):
                continue

            props = {}
            for field in bus_stops_prop_fields:
                val = getattr(row, field)
                if not pd.isna(val):
                    props[field] = val

            point = {
                'type': 'Feature',
                'properties': props,
                'geometry': {
                    'type': 'Point',
                    'coordinates': [row.Longitude, row.Latitude],
                }
            }

            prefix = ",\n" if i != 0 else "\n"
            f.write(prefix + json.dumps(point))
            wrote_count += 1

            if wrote_count % 20_000 == 0:
                print(f"Wrote {wrote_count}/~{approx_row_count} (~{round(wrote_count / approx_row_count * 100)}%)")

        f.write('\n]}\n')
    print(f"Wrote {wrote_count} stops to {bus_stops_geojson}")
    print(f"Skipped {skipped_stop_count} stops as not in gtfs")

    bus_stops_pmtiles = path.join(scratch, "bus_stops_uk.pmtiles")
    print(f"Generating {bus_stops_pmtiles}")
    subprocess.run([
        "tippecanoe",
        "--output", bus_stops_pmtiles, "--force",
        "--name", "Bus Stops (UK)",
        "--description", "Active bus stops in England, Scotland, and Wales",
        "--attribution", attribution,
        "--layer", "default",
        "--generate-ids",
        "-zg", "--extend-zooms-if-still-dropping",
        "--no-tile-stats",
        bus_stops_geojson,
    ], check=True)
    print(f"Wrote {bus_stops_pmtiles}")

    with open(bus_stops_pmtiles, "rb") as f:
        storage_url = "https://uk.storage.bunnycdn.com/plantopo/bus_stops_uk.pmtiles"
        print(f"Uploading {storage_url}")
        upload_r = requests.put(
            storage_url,
            headers={"AccessKey": bunny_storage_key},
            data=f,
        )
        upload_r.raise_for_status()
        print(f"Uploaded {storage_url}")

        purge_url = "https://plantopo-storage.b-cdn.net/bus_stops_uk.pmtiles"
        print(f"Purging {purge_url}")
        purge_r = requests.get(
            "https://api.bunny.net/purge?" + urllib.parse.urlencode({"url": purge_url}),
            headers={"AccessKey": bunny_key}
        )
        purge_r.raise_for_status()

    print("All done")


def download_dft_gtfs(username: str, password: str, out: str):
    s = requests.Session()
    s.headers.update({"User-Agent": "github.com/dzfranklin/naptan-map (daniel@danielzfranklin.org)"})

    # GET /account/login for csrf cookie and csrf input value

    login_resp = s.get("https://data.bus-data.dft.gov.uk/account/login/")
    login_resp.raise_for_status()

    login_doc = Soup(login_resp.content, features="html.parser")
    csrf_node = login_doc.select_one("input[name=csrfmiddlewaretoken]")
    if not csrf_node:
        raise RuntimeError("could not find csrf node")

    csrf_token = csrf_node.get("value")
    if not csrf_token:
        raise RuntimeError("expected csrf node to have attr value")

    # POST /account/login for cookie

    login_data = {
        "csrfmiddlewaretoken": csrf_token,
        "login": username,
        "password": password,
        "submit": "submit",
    }
    login_resp = s.post(
        "https://data.bus-data.dft.gov.uk/account/login/",
        data=login_data,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://data.bus-data.dft.gov.uk/account/login/",
            "Origin": "https://data.bus-data.dft.gov.uk"
        }
    )
    login_resp.raise_for_status()

    # GET /timetable/download/gtfs-file/all

    download_url = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/all/"
    print(f"Downloading {download_url}")
    download_resp = s.get(
        download_url,
        headers={
            "Referer": "https://data.bus-data.dft.gov.uk/timetable/download/",
            "Origin": "https://data.bus-data.dft.gov.uk",
        },
        stream=True,
    )
    download_resp.raise_for_status()
    with download_resp, open(out, "wb+") as f:
        for chunk in download_resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print(f"Downloaded {download_url} to {out}")


if __name__ == "__main__":
    args_p = argparse.ArgumentParser()

    # Optional arguments to assist in local debugging
    args_p.add_argument("--naptan-csv")
    args_p.add_argument("--dft-gtfs")
    args_p.add_argument("--scratch")

    args = args_p.parse_args()

    with tempfile.TemporaryDirectory() as scratch_dir:
        if args.scratch is not None:
            scratch_dir = args.scratch

            if os.path.exists(scratch_dir):
                for entry in os.scandir(scratch_dir):
                    if entry.is_file():
                        os.remove(entry.path)
                    elif entry.is_dir():
                        shutil.rmtree(entry.path)
            else:
                os.makedirs(scratch_dir)

            print(f"Using {scratch_dir} as scratch")

        main(
            scratch=scratch_dir,
            naptan_csv_path=args.naptan_csv,
            dft_gtfs_path=args.dft_gtfs,
        )
