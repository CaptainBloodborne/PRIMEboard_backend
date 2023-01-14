# encoding=utf-8
import logging
import sys

from .elasticsearcher import Preset
import requests

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("pop_merger_sorter.py")
logging.getLogger("chardet.charsetprober").disabled = True


def define_subject(subject_id, relevance_coef: int, nms: list):
    for nm in nms:
        if nm["fields"].get("subjectId") == subject_id:
            nm["fields"]["score"] = nm["fields"]["score"] * 10 * relevance_coef


def preset_gen(path, preset_range_1: int, preset_range_2: int):
    """ """
    import os
    from random import randint

    from bash import bash

    preset = str(randint(preset_range_1, preset_range_2))

    os.chdir(path)
    bash("git checkout ru")
    if_exist_ru = bash(f"git grep 'preset={preset}'").value()
    logging.debug(f"bash answer: {if_exist_ru}")
    bash("git checkout common")
    if_exist_common = bash(f"git grep 'preset={preset}'").value()
    logging.debug(f"bash answer: {if_exist_common}")

    while if_exist_ru or if_exist_common:
        preset = str(randint(preset_range_1, preset_range_2))
        bash("git checkout ru")
        if_exist_ru = bash(f"git grep 'preset={preset}'").value()
        logging.debug(f"bash answer: {if_exist_ru}")
        bash("git checkout common")
        if_exist_common = bash(f"git grep 'preset={preset}'").value()
        logging.debug(f"bash answer: {if_exist_common}")

    return preset


def adult_searcher(nm: str, path):
    """
    Deprecated
    """
    import pandas as pd
    import os

    os.chdir(path)

    if len(nm) == 7:
        try:
            adult_f = pd.read_csv(
                f"{nm[:2]}-details.csv",
                index_col=[0],
                names=[0, 1, 2, 3, 4, 5, 6, 7],
            )
        except FileNotFoundError:
            return f"Номенклатура не найдена! Необходимо обновить каталог."
    elif len(nm) == 8:
        try:
            adult_f = pd.read_csv(
                f"{nm[:2]}-details.csv",
                index_col=[0],
                names=[0, 1, 2, 3, 4, 5, 6, 7],
            )
        except FileNotFoundError:
            return f"Номенклатура не найдена! Необходимо обновить каталог."
    else:
        raise FileNotFoundError

    try:
        if adult_f.loc[int(nm)][5] == 1:
            return "adult"
        else:
            return "common"
    except KeyError as err:
        return f"Номенклатура не найдена: ({err})"


def normalize(req):
    import requests
    from urllib.parse import urlencode, quote_plus

    query = urlencode(
        {"query": f"{req}"},
        quote_via=quote_plus,
    )

    url = "http://textprocesser.wbx-search.svc.k8s.wbxsearch-dp/normalize?"

    response_tokenizer = requests.get(
        url + query
    )

    response_tokenizer.raise_for_status()

    return response_tokenizer.text


def get_context(req: str = "", ctx: str = ""):
    import requests

    if ctx == "":
        context_response = requests.get(
            f"http://sbm-detector.wbx-search.svc.k8s.wbxsearch-dl/getcontext?query={req}",
        )

        context_response.raise_for_status()

        if context_response.status_code != 200:
            return [None, None]

        return [context_response.text, None]

    elif req == "":
        search_context_response = requests.get(
            f"http://sbm-detector.wbx-search.svc.k8s.wbxsearch-dl/bycontext?query={ctx}",
        )

        search_context_response.raise_for_status()

        if search_context_response.status_code != 200:
            return [None, None]

        search_context_j = search_context_response.json()

        search_context_list = [
            (contx.get("subject_name"), contx.get("subject_id"), contx.get("relevance"))
            for contx in search_context_j
        ]

        return [None, search_context_list]

    context_response = requests.get(
        f"http://sbm-detector.wbx-search.svc.k8s.wbxsearch-dl/getcontext?query={req}",
    )

    context_response.raise_for_status()

    context_response_text = context_response.text

    if context_response.status_code != 200:
        context_response_text = None

    search_context_response = requests.get(
        f"http://sbm-detector.wbx-search.svc.k8s.wbxsearch-dl/bycontext?query={ctx}",
    )

    search_context_response.raise_for_status()

    search_context_j = search_context_response.json()

    search_context_list = [
        (contx.get("subject_name"), contx.get("subject_id"), contx.get("relevance"))
        for contx in search_context_j
    ]

    if search_context_response.status_code != 200:
        search_context_list = None

    return [context_response_text, search_context_list]


def exact_search(query: str):
    import requests

    params = {
        "query": query,
        "debug": "false",
    }
    headers = {"accept": "application/json"}
    user_request = (
        f"http://exactmatch-common.wbxsearch-internal.svc.k8s.wbxsearch-dl/v2/search?"
    )
    resp = requests.get(user_request, params=params, headers=headers)
    resp_json = resp.json()

    return resp_json


def get_open_bucket():
    import pandas as pd
    import csv

    cache_buckets = [
        "bucket-447",
        "bucket-448",
        "bucket-449",
        "bucket-450",
        "bucket-451",
        "bucket-452",
        "bucket-453",
        "bucket-454",
        "bucket-455",
    ]

    df = pd.read_csv(
        "/usr/src/datastore_common/buckets/buckets-info.csv",
        delimiter="|",
        quoting=csv.QUOTE_NONE,
        index_col="Имя бакета",
    )

    df["дата"] = df["дата"].fillna(0)

    opened_buckets = df.loc[df["открытость"] == True].transpose().to_dict()
    opened_buckets_list = [
        (
            bucket,
            [
                str(
                    opened_buckets.get(bucket).get("доминирующие сабжекты(сепаратор ;)")
                ),
                str(opened_buckets.get(bucket).get("категория")),
                str(opened_buckets.get(bucket).get("кол-во НМ общее")),
                str(
                    opened_buckets.get(bucket).get(
                        "кол-во дом. род. сабжектов в запросе(в процентах)"
                    )
                ),
                str(opened_buckets.get(bucket).get("кол-во уникальных нм")),
                str(opened_buckets.get(bucket).get("контекст(на будущее)")),
                str(opened_buckets.get(bucket).get("дата")),
                str(opened_buckets.get(bucket).get("открытость")),
            ],
        )
        for bucket in opened_buckets
        if bucket not in cache_buckets
    ]

    opened_buckets_list.insert(
        0,
        (
            "Имя бакета",
            [
                "Доминирующие сабжекты",
                "Категория",
                "кол-во НМ общее",
                "кол-во дом. род. сабжектов в запросе",
                "кол-во уникальных нм",
                "контекст(на будущее)",
                "дата",
                "открытость",
            ],
        ),
    )

    return opened_buckets_list


def get_content_info(nms_list: list):
    url = f"https://card.wb.ru/cards/detail"

    GET_ARGS = {
        "spp": ["0"],
        "regions": ["68,64,83,4,38,80,33,70,82,86,75,30,69,48,22,1,66,31,40,71"],
        "stores": [
            "117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043"
        ],
        "pricemarginCoeff": ["1.0"],
        "reg": ["0"],
        "appType": ["55"],
        "emp": ["0"],
        "locale": ["ru"],
        "lang": ["ru"],
        "curr": ["rub"],
        "couponsGeo": ["12,3,18,15,21"],
        "dest": ["-1029256,-102269,-1278703,-1255563"],
        "nm": ";".join(nms_list)
    }

    resp = requests.get(url, params=GET_ARGS)

    resp.raise_for_status()

    nms = resp.json()["data"].get("products")

    # nms = [
    #     {nm.get("id"): nm} for nm in nms
    # ]

    img_pattern = "https://images.wbstatic.net/c246x328/new"

    nms = {
        nm.get("id"): {
            "nm": (nm_id := nm.get("id")),
            "name": nm.get("name"),
            # "subject": nm.get("subject"),
            "subjectId": nm.get("name"),
            # "parentSubjectId": nm.get("parentSubjectId"),
            # "brand": nm.get("brand"),
            "brandId": nm.get("brandId"),
            "kind": nm.get("kindId"),
            "url": Preset.to_html(
                img_pattern
                + "/"
                + str(nm_id - nm_id % 10000)
                + "/"
                + str(nm_id)
                + "-1.jpg",
                nm_id),
        } for nm in nms
    }

    # [
    #     {
    #         "nm": (nm_id := nm.get("id")),
    #         "url": self.to_html(
    #             img_pattern
    #             + "/"
    #             + str(nm_id - nm_id % 10000)
    #             + "/"
    #             + str(nm_id)
    #             + "-1.jpg",
    #             nm_id,
    #         )
    #     } for nm in self._response.get("products")
    # ]

    return nms


def exact_common():
    return requests.get("https://search.wb.ru/exactmatch/v2/common?query=sela свитер").json()
