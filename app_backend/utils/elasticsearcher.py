import requests
import logging
import json
from collections import Counter

from typing import Any, List

from .work_piece_parser import WorkPiece

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("elasticsearcher")


class Nm:
    nm_id: int
    subject_id: int
    brand_id: int
    stock_exists: int
    score: float

    def __init__(
            self,
            nm_id: int,
            subject_id: int,
            brand_id: int,
            stock_exists: int,
            score: float,
    ):
        self.nm_id = nm_id
        self.subject_id = subject_id
        self.brand_id = brand_id
        self.stock_exists = stock_exists
        self.score = score


class Preset:
    _products: List[dict]
    _total: int
    _in_stock: bool
    _bucket: List[dict]
    _miner: str
    _token: str

    def __init__(
            self,
            in_stock: bool = False,
            miner: str = "elasticsearch",
    ):

        self._buckets: list = []
        self._in_stock = in_stock
        self._data = list()
        self._miner = miner

    def run_miner(
            self,
            workpiece: str,
            searcher_api: str = "http://miner-context.wbxsearch-internal.svc.k8s.wbxsearch-dl/search"
    ):
        response = elastic_search(workpiece, searcher_api=searcher_api)
        self._products = response.get("products")
        self._total = response.get("total")

    @staticmethod
    def to_html(url: str, nm: str):
        html_tag = (
            f"<a href='https://www.wildberries.ru/catalog/{str(nm)}/detail.aspx?targetUrl=SP' target='_blank'>"
            f"<img src='{url}' width='137' height='182'  alt='{str(nm)}'></a>"
        )

        return html_tag

    def make_preset(self):
        img_pattern = "https://images.wbstatic.net/c246x328/new"

        self._data = [
            {
                "nm_id": (nm_id := nm.get("id")),
                # "subject": nm["fields"].get("subject", "empty")[0],
                "subject_id": nm["fields"].get("subjectId"),
                # "parentSubjectId": nm["fields"].get("parentSubjectId", "empty")[0],
                "brand_id": nm["fields"].get("brandId"),
                "stock_exists": nm["fields"].get("stockExists"),
                # "scoreNorma": nm["fields"].get("scoreNorma"),
                # "elasticScore": nm["fields"].get("elasticScore"),
                "score": nm["fields"].get("score"),
                # "imgUrl": self.to_html(
                #     img_pattern
                #     + "/"
                #     + str(nm_id - nm_id % 10000)
                #     + "/"
                #     + str(nm_id)
                #     + "-1.jpg",
                #     nm_id,
                # )
            } for nm in self._products
        ]

    def get_page(self, page: int):
        num = (page - 1) * 100
        return self._data[num: num + 100]

    @property
    def get_preset(self):
        return self._data

    @property
    def get_preset_total(self):
        return len(self._products)

    @property
    def get_subjects(self):
        # subjects = [nm.get("subjectId") for nm in self._data]
        # sorted_counter = Counter(subjects)
        # url_name = "http://localhost:5000/get_subject_name?subject="
        # subject_names = {
        #     subject: requests.get(url_name + str(subject)).text for subject in list(sorted_counter.keys())
        # }
        # subjects_count = [
        #     f"{subject_names.get(k)}({v})"
        #     for k, v in sorted_counter.most_common()
        # ]
        #
        # return subjects_count, subject_names

        if self._miner == "elasticsearch":
            subjects = [
                nm.get("subjectId")
                for nm in self._data
            ]

            sorted_counter = Counter(subjects)
            # subjects_count = [
            #     f"{k}({v})"
            #     for k, v in sorted_counter.most_common()
            # ]

            return sorted_counter.most_common()
        elif self._miner == "context":
            subjects = [
                nm.get("subjectId")
                for nm in self._data
            ]

            sorted_counter = Counter(subjects)
            # url_name = "http://localhost:5000/get_subject_name?subject="
            # subjects_count = [
            #     f"{requests.get(url_name + str(k)).text}({v})"
            #     for k, v in sorted_counter.most_common()
            # ]

            return sorted_counter.most_common()

    @property
    def get_brands(self):
        if self._miner == "elasticsearch":
            brands = [
                nm.get("brandId")
                for nm in self._data
            ]

            sorted_counter = Counter(brands)
            # subjects_count = [
            #     f"{k}({v})"
            #     for k, v in sorted_counter.most_common()
            # ]

            return sorted_counter.most_common()
        elif self._miner == "context":
            brands = [
                nm.get("brandId")
                for nm in self._data
            ]

            sorted_counter = Counter(brands)
            # url_name = "http://localhost:5000/get_subject_name?subject="
            # subjects_count = [
            #     f"{requests.get(url_name + str(k)).text}({v})"
            #     for k, v in sorted_counter.most_common()
            # ]

            return sorted_counter.most_common()
        # if self._miner == "elasticsearch":
        #     brands = [
        #         f'{nm["fields"].get("brandId")}, {nm["fields"].get("brand")}'
        #         for nm in self._products
        #         if nm.get("fields").get("brand")
        #     ]
        #
        #     sorted_counter = Counter(brands)
        #     brands_count = [
        #         f"{k}({v})"
        #         for k, v in sorted_counter.most_common()
        #     ]
        #
        #     return brands_count
        #
        # elif self._miner == "context":
        #     brands = [
        #         nm["fields"].get("brandId")
        #         for nm in self._products
        #         if nm["fields"].get("brandId")
        #     ]
        #
        #     sorted_counter = Counter(brands)
        #     brands_count = [
        #         f"{k}({v})"
        #         for k, v in sorted_counter.most_common()
        #     ]
        #
        #     return brands_count

    @property
    def get_nms_list(self):
        nms = [nm.get("id") for nm in self._products]

        return nms

    @property
    def get_in_stock(self):
        counter = 0
        for nm in self._data:
            if nm.get("stockExists"):
                counter += 1

        return counter

    @property
    def get_elastic_total(self):
        return len(self._total)


def elastic_search(
        work_piece: str,
        searcher_api: str = "http://elastic-miner.wbx-search.svc.k8s.wbxsearch-dp:80/search",
):
    """
    Send request to elasticsearcher and collect nms with exact fields

    :param work_piece: string from datastore with predefined params
    :param searcher_api: change if you need to test new features
    :return: Response object with mined nms
    """
    work_piece_instance = WorkPiece(work_piece)

    request_body = work_piece_instance.parse_miner_args()

    request_body["fields"] = [
        "score",
        "scoreNorma",
        "elasticScore",
        "name",
        "subject",
        "subjectId",
        "parentSubjectId",
        "brandId",
        "brand",
        "stockExists",
        "kind",
    ]

    request_body["presetId"] = int(work_piece_instance.get_preset_id)

    if "irrelevantScorePercent" not in request_body:
        if "contextSubject" in request_body:
            request_body["irrelevantScorePercent"] = 20
        else:
            request_body["irrelevantScorePercent"] = 8

    headers = {"accept": "application/json"}
    user_request = searcher_api

    resp = requests.post(user_request, data=json.dumps(request_body), headers=headers)

    logger.info(f"{request_body}")

    resp.raise_for_status()

    return resp.json()


# def catalog_miner(
#     work_piece: str,
#     searcher_api: str = "http://miner-catalog.wbxsearch-internal.svc.k8s.wbxsearch-dl/search",
#     sort: bool = True,
#     locale: str = "ru",
#     debug: bool = False,
# ):
#     """
#     Make request to catalog miner and get nms with info
#     """
#     request_body = {
#         "MinerArgs": {"URLs": urls},
#         "limit": limit,
#         "sort": sort,
#         "locale": locale,
#         # "query": "скейтборд",
#         "debug": debug,
#     }
#
#     headers = {
#         "Accept": "application/json",
#         "Content-Type": "application/json",
#     }
#
#     url = "http://catalog.wbx-search.svc.k8s.wbxsearch-dl/api/productsURI"
#
#     resp = requests.post(
#         url,
#         data=json.dumps(request_body),
#         headers=headers,
#     )
#
#     resp.raise_for_status()
#
#     if resp.status_code != 200:
#         return resp.status_code, "Error in getting info from catalog miner"
#
#     return resp


class Buckets:
    FIELDS: dict = {
        "bucket_name": 0,
        "uniq_before_add": 1,
        "uniq_after_add": 2,
        "increasing_uniqueness": 3,
        "unique_nm_will_be_added": 4,
        "total_before_add": 5,
        "total_after_add": 6,
    }

    def __init__(
            self,
            buckets_dict: dict,
    ):
        self._buckets_dict = buckets_dict
        self._buckets = [
            (
                bucket.get("bucket_name"),
                bucket.get("uniq_before_add"),
                bucket.get("uniq_after_add"),
                bucket.get("increasing_uniqueness"),
                bucket.get("unique_nm_will_be_added"),
                bucket.get("total_before_add"),
                bucket.get("total_after_add"),
            )
            for bucket in self._buckets_dict
            if bucket.get("uniq_after_add") < 1000000
               and bucket.get("total_after_add") < 8000000
        ]

    def add_header(self):
        self._buckets.insert(
            0,
            (
                "Имя бакета",
                "Уникальных нм до",
                "Уникальных нм после",
                "Повышение уникальности",
                "Уникальных нм добавлено",
                "Тотал нм перед",
                "Тотал нм после",
            ),
        )

    @property
    def get_buckets(self):
        return self._buckets

    def filter_buckets(self, field: str):
        self._buckets.sort(key=lambda x: x[self.FIELDS[field]])


def bucket_info(nms_list: list):
    url = "http://buckets-nm-info.wbx-search.svc.k8s.wbxsearch-dl/api/v1/find_bucket"

    request_body = {"nms": nms_list}

    headers = {"accept": "application/json"}

    try:
        resp = requests.post(
            url, data=json.dumps(request_body), headers=headers, timeout=30
        )
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        return None
    except requests.exceptions.HTTPError:
        return None
    except requests.exceptions.ConnectionError:
        return None

    if resp.status_code == 204:
        return None
    elif resp.status_code != 200:
        return None

    resp_json = resp.json()

    return resp_json


# Old version of visualization for reference
def elastic_search_old(
        user_request: str,
        irr_score: int = 8,
        max_product: int = 60000,
        filters: str = None,
        only_total: bool = False,
        searcher_api: str = "http://elastic-miner.wbx-search.svc.k8s.wbxsearch-dp:80/search",
        total_api: str = "http://elastic-miner.wbx-search.svc.k8s.wbxsearch-dp/total",
):
    request_body = {
        "query": user_request,
        "irrelevantScorePercent": irr_score,
        "maxProduct": max_product,
        "fields": [
            "score",
            "elasticScore",
            "name",
            "subject",
            "subjectId",
            "parentSubjectId",
            "brandId",
            "brand",
            "stockExists",
            "kind",
        ],
    }

    if filters:
        for param in filters.split(";"):
            add_to_req_body = param.split("=")
            filter_name = add_to_req_body[0]
            filter_value = add_to_req_body[1]
            if filter_value.isdigit():
                filter_value = int(filter_value)

            if filter_value == "true" or filter_value == "True":
                filter_value = True
            elif filter_value == "false" or filter_value == "False":
                filter_value = False
            request_body.update({filter_name: filter_value})

    headers = {"accept": "application/json"}
    user_request = searcher_api
    if only_total:
        user_request = total_api
        resp = requests.post(
            user_request, data=json.dumps(request_body), headers=headers
        )
        resp_json_total = resp.json()["total"]
        return resp_json_total
    resp = requests.post(user_request, data=json.dumps(request_body), headers=headers)

    logger.info(f"{request_body}")

    return resp
