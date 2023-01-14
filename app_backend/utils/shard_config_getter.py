import bz2
import concurrent.futures
import csv
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

import redis
import requests
import urllib.parse
from requests import Session
from dotenv import load_dotenv
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.suggestion import Suggestion
from schedule import every, repeat, run_pending

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("config_parser")

load_dotenv()

redis_shard = redis.Redis(host="redis-1", port=6379, db=0)


def map_shard(shard: str):
    split_filters_row = shard.split("filters: ")[1].split("|")

    groups = list()

    for group in split_filters_row:
        filter_group = group.split("&")

        for filt in filter_group:
            if len(filt.split("=")) == 1:
                filter_group[filter_group.index(filt)] = "omit_f=" + filt

        groups.append(
            {
                c_filter.split("=")[0]: c_filter.split("=")[1].split(",")
                for c_filter in filter_group
            },
        )

    return groups


def download_config_file(filename: str):
    url = (
        f"https://wbxgit.wb.ru/"
        f"api/v4/projects/3/repository/files/ansible-playbooks%2Fvars%2Fdefault%2F{filename}/raw?ref=master"
    )
    headers = {"PRIVATE-TOKEN": os.environ.get("GITLAB_3_TOKEN")}
    logger.info(f"Start downloading {filename}...")
    gitlab_r = requests.get(url, headers=headers)
    gitlab_r_list = gitlab_r.content.decode("utf-8").split("\n")
    filters = gitlab_r_list[0]

    for row in gitlab_r_list:
        if "filters" in row:
            filters = row

    return filters, filename


@repeat(every(45).minutes)
def set_config():
    # Get correct filenames list to download from repo
    config_files = list()

    page = 1

    while True:
        gitlab_resp = requests.get(
            f"https://wbxgit.wb.ru/api/v4/projects/3/repository/tree?path=ansible-playbooks/vars/default&page={page}",
            headers={
                "PRIVATE-TOKEN": os.environ.get("GITLAB_3_TOKEN"),
            },
        )
        gitlab_resp.raise_for_status()
        logger.debug(gitlab_resp.status_code)
        logger.info(f"Page is: {page}")
        gitlab_json = gitlab_resp.json()
        if gitlab_json:
            file_list = [
                f
                for file in gitlab_json
                if "sellers" not in (f := file.get("name"))
                and "suppliers" not in f
                and "brands" not in file.get("name")
                and "default" not in f
                and ".md" not in file.get("name")
                and "config" not in f
                and "top100.yaml" not in file.get("name")
            ]
            config_files.extend(file_list)
        else:
            break
        page += 1

    # Remove unnecessary files
    for config_file in range(100):
        if (path := f"s{config_file}.yaml") in config_files:
            config_files.remove(path)

    configs_dict = dict()

    error_counter = 0
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = []

        for filename in config_files:
            futures.append(executor.submit(download_config_file, filename=filename))

        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                configs_dict[res[1][:-5]] = res[0]
            except Exception as err:
                error_counter += 1
                logger.warning(f"{err} occurred, while downloading")
            else:
                logger.info(f"{res[0]}: Download finished.")

    for key, value in configs_dict.items():
        configs_dict[key] = map_shard(value)

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = list()

        for values in configs_dict.values():
            futures.append(
                executor.submit(
                    update_shard_ext,
                    shard=values,
                ),
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
            except Exception as err:
                error_counter += 1
                logger.error(f"{err} occurred, while getting ext value")
            else:
                logger.info(f"Value acquired: {res}")

    subjects_raw = bz2.decompress(redis_shard.get("subjects"))

    subjects_raw_decoded = subjects_raw.decode("utf-8")
    subjects_dict = json.loads(subjects_raw_decoded)

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = list()

        for value in configs_dict.values():
            futures.append(
                executor.submit(
                    update_shard_subject,
                    shard=value,
                    subjects_dict=subjects_dict,
                ),
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
            except Exception as err:
                error_counter += 1
                logger.error(f"{err} occurred, while getting subject value")
            else:
                logger.info(f"Subject updated: {res}")

    logger.info("All subjects updated!")

    for key, value in configs_dict.items():
        for group in value:

            subj_s = group.get("subjects")
            ext_s = group.get("exts")
            kind_s = group.get("kinds")

            group_content = {
                "name": key,
            }

            if subj_s:
                group_content["subjects"] = " ' ".join(
                    list(
                        map(
                            lambda x: x.split("(")[0].lower() if x else "nil",
                            list(group.get("subjects").values()),
                        ),
                    ),
                )
            if ext_s:
                if isinstance(group.get("exts"), list):
                    pass
                else:
                    group_content["exts"] = " , ".join(list(group.get("exts").values()))

            if kind_s:
                group_content["kinds"] = " , ".join(list(group.get("kinds")))

            redis_shard.hset(
                f"shard:{key}:{value.index(group)}",
                mapping=group_content,
            )

    redis_shard.set(
        "shard-configs",
        bz2.compress(json.dumps(configs_dict).encode("utf-8")),
    )


def ask_content(ext: str):
    headers = {
        "User-Agent": "PostmanRuntime/7.29.2",
    }
    logger.info(f"Calling exts api {ext}...")
    url = f"https://content.wildberries.ru/api/v3/product-cache/directory/added-options-ext?addedOptionsExtId={ext}"
    resp = requests.get(url, headers=headers, timeout=10)

    ext_name = resp.json().get("name").lower()

    logger.info(f"Finish calling exts {ext}")

    return ext_name


def update_shard_ext(shard: list):
    logger.info(f"Start updating {shard}")
    for group in shard:
        is_ext = group.get("exts")

        if is_ext:
            group["exts"] = {ext: ask_content(ext) for ext in is_ext}
    logger.info(f"Finish updating {shard}")


def update_shard_subject(shard: list, subjects_dict: dict):
    for group in shard:
        subjects = group.get("subjects")
        if subjects:
            group["subjects"] = {subj: subjects_dict.get(subj) for subj in subjects}


@repeat(every(6).hours)
def get_subjects_list():
    with Session() as content_session:
        content_session.cookies.update(
            {
                "x-service-id": os.environ.get("CONTENT_TOKEN"),

            }
        )
        content_session.headers.update(
            {
                "user-session": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.42",
                "Content-Type": "application/json",
            }
        )

        url = "https://content-card-datalayer.wb.ru"

        body = {"jsonrpc": "2.0", "method": "directory.getallsubjects", "id": 1}

        subjects_content = content_session.post(url, json=body)

        logger.debug(f"Response is <{subjects_content.json()}>")

    subjects_content_json = subjects_content.json().get("result").get("data")

    subjects_dict = {
        str(elem.get("id")): f"{elem.get('name')}"  # ({elem.get('nameSf')})"
        for elem in subjects_content_json
    }

    subjects__by_name_dict = {
        value.split("(")[0]: key for key, value in subjects_dict.items()
    }

    redis_shard.set("subjects", bz2.compress(json.dumps(subjects_dict).encode("utf-8")))
    redis_shard.set(
        "subjects_by_name",
        bz2.compress(json.dumps(subjects__by_name_dict).encode("utf-8")),
    )

    logger.info("Subjects loaded!")


# @repeat(every(30).minutes)
def shard_indexer():
    logger.info("Calling content-card-datalayer...")
    with Session() as content_session:
        content_session.cookies.update(
            {
                "x-service-id": os.environ.get("CONTENT_TOKEN"),

            }
        )
        content_session.headers.update(
            {
                "user-session": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.42",
                "Content-Type": "application/json",
            }
        )

        url = "https://content-card-datalayer.wb.ru"

        body = {"jsonrpc": "2.0", "method": "directory.getallsubjects", "id": 1}

        subjects_content = content_session.post(url, json=body)

        logger.debug(f"Response is <{subjects_content.json()}>")

    subjects_content_json = subjects_content.json().get("result").get("data")

    logger.info("Get subjects from content!")

    for subject in subjects_content_json:
        subject["id"] = str(subject.get("id"))
        subject["parentID"] = str(subject.get("parentID"))
        subject["name"] = subject["name"].lower()
        subject["nameSf"] = subject["nameSf"].lower()
        s_id = str(subject.get("id"))

        redis_shard.json().set(
            f"subject:{s_id}",
            Path.root_path(),
            subject,
        )
        redis_shard.ft().sugadd("auto_subjects", Suggestion(subject.get("nameSf")))

    logger.info("Finish with suggestions!")

    schema = (
        TextField("$.name", as_name="name", sortable=True),
        TextField("$.id", as_name="id", sortable=True),
        # TextField("$.parentID", as_name="parentID"),
        TextField("$.nameSf", as_name="nameSf"),
    )

    try:
        redis_shard.ft("idx:subjects").create_index(
            fields=schema,
            definition=IndexDefinition(
                prefix=["subject:"],
                index_type=IndexType.JSON,
                language_field="russian",
                language="russian",
            ),
        )
    except redis.exceptions.ResponseError:
        logger.warning("Index already exists!")

    shards_schema = (
        TextField("subjects", weight=10.0),
        TextField(
            "exts",
            weight=5.0,
        ),
        TextField("kinds", weight=5.0),
    )

    try:
        redis_shard.ft("idx:shards").create_index(
            fields=shards_schema,
            definition=IndexDefinition(
                prefix=["shard:"],
                index_type=IndexType.HASH,
                language_field="russian",
                language="russian",
            ),
        )
    except redis.exceptions.ResponseError:
        logger.warning("Index already exists!")


if __name__ == "__main__":
    get_subjects_list()
    shard_indexer()
    set_config()
    while True:
        run_pending()
        time.sleep(5)
