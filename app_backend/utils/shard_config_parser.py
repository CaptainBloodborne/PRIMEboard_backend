import bz2
import concurrent.futures
import json
import logging

import requests
from redis import Redis

redis_shard = Redis(host="redis-1", port=6379, db=0)

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("config_parser")


def ask_content(ext: str):
    headers = {
        "User-Agent": "PostmanRuntime/7.29.2",
    }
    url = f"https://content.wildberries.ru/api/v3/product-cache/directory/added-options-ext?addedOptionsExtId={ext}"
    resp = requests.get(url, headers=headers)

    ext_name = resp.json().get("name")

    return ext, ext_name


def thread_requests(values_lst: list, func):
    error_counter = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = list()

        val_dict = dict()

        for value in values_lst:
            futures.append(
                executor.submit(
                    func,
                    ext=value,
                ),
            )

        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                val_dict[res[0]] = res[1]
            except Exception as err:
                error_counter += 1
                logger.error(f"{err} occurred, while getting ext value")
            else:
                logger.info("Value acquired.")

    return val_dict


# Show only shards
def get_shard_name(value: str):
    configs_dict_raw = bz2.decompress(redis_shard.get("shard-configs"))

    configs_dict_decoded = configs_dict_raw.decode("utf-8")
    configs_dict = json.loads(configs_dict_decoded)

    output = dict()

    for key, values in configs_dict.items():
        output[key] = dict()
        logger.debug(f"Key is {key}\nValues are: {values}")
        if (
            len(values) == 1
            and "subjects" in values[0]
            and value in values[0].get("subjects")
            and len(values[0]) == 1
        ):
            output[key] = "fullsubject"

        elif len(values) == 1 and len(values[0]) > 1:
            for group in values:
                if (
                    len(group) == 1
                    and value in group.get("subjects")
                    and group.get("subjects") is not None
                ):
                    output[key] = "fullsubject"
                    logger.debug(f"Group is {group}\nValues are {values}")
                elif value not in group.get("subjects"):
                    continue
                else:
                    for s_filter in group:
                        if s_filter != "subjects":
                            output[key].update(
                                {
                                    str(values.index(group))
                                    + s_filter: group.get(s_filter),
                                },
                            )

        elif len(values) > 1:

            for group in values:
                if (
                    len(group) == 1
                    and value in group.get("subjects")
                    and group.get("subjects") is not None
                ):
                    output[key] = "fullsubject"
                    logger.debug(f"Group is {group}\nValues are {values}")
                elif value not in group.get("subjects"):
                    continue
                elif output[key] == "fullsubject":
                    continue
                else:
                    for s_filter in group:
                        if s_filter != "subjects":
                            logger.warning(f"Output[key] is: {output[key]}")
                            output[key].update(
                                {
                                    str(values.index(group))
                                    + s_filter: group.get(s_filter),
                                },
                            )

    # Make list to add subject name to output
    output_value = list()

    # Get subjects from redis
    subjects_raw = bz2.decompress(redis_shard.get("subjects"))

    subjects_raw_decoded = subjects_raw.decode("utf-8")
    subjects_dict = json.loads(subjects_raw_decoded)

    output_value.append(
        {
            "Subject name": subjects_dict.get(value),
        },
    )
    output_value.append(
        {key: value for key, value in output.items() if len(value)},
    )
    return output_value
