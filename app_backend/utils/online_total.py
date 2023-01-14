import json
import logging
import requests
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urlencode, quote_plus

retries = Retry(
    total=20, backoff_factor=0.1,
    status_forcelist=[500, 502, 503, 504],
)  # настройки переподключений при указанных ошибках requests


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger("online_total")


def tokenizer(human_query: str):
    # query = urlencode(
    #     {"query": f"{human_query} 11111111"},
    #     quote_via=quote_plus,
    # )
    api_url = "http://txt2vec.wbx-search.svc.k8s.wbxsearch-dp/api/v2/normalize"

    with requests.session() as session:
        session.headers["User-Agent"] = "insomnia/2022.2.1"
        session.mount("http://", HTTPAdapter(max_retries=retries))

        # payload = {
        #     "spp": ["0"],
        #     "regions": ["68,64,83,4,38,80,33,70,82,86,75,30,69,48,22,1,66,31,40,71"],
        #     # "stores": [
        #     #     "117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043"
        #     # ],
        #     "pricemarginCoeff": ["1.0"],
        #     "reg": ["0"],
        #     "appType": ["55"],
        #     "emp": ["0"],
        #     "locale": ["ru"],
        #     "lang": ["ru"],
        #     "curr": ["rub"],
        #     "couponsGeo": ["12,3,18,15,21"],
        #     "dest": ["-1029256,-102269,-1278703,-1255563"],
        #     "resultset": ["filters"],
        #     "sort": ["popular"],
        #     "suppressed": "true"
        # }

        data = {
            "text": str(human_query)
        }

        response = session.post(api_url, data=json.dumps(data))

        logger.info(response.url)
        logger.info(human_query)

        response.raise_for_status()

        j_data = response.json()

        tokens = list()

        try:
            tokens_list = j_data["tokens"]
            for token in tokens_list:
                tokens.append(f"_t{tokens_list.index(token)}={token[1]}")

            return tokens

        except Exception as e:
            logger.warning(f"{e}")
            return None


def get_query(query: str) -> str:
    with requests.session() as session:
        session.headers["User-Agent"] = "insomnia/2022.2.1"
        session.mount("http://", HTTPAdapter(max_retries=retries))

        endpoint = "https://search.wb.ru/merger/filters?"
        vectors = tokenizer(query)
        if vectors is not None:
            vectors_url = "&" + "&".join(vectors)

            payload = {
                "filters": "xsubject",
                "spp": 0,
                "pricemarginCoeff": 1.0,
                "reg": 1,
                "appType": 55,
                "offlineBonus": 0,
                "onlineBonus": 0,
                "emp": 1,
                "locale": "ru",
                "lang": "ru",
                "curr": "rub",
            }
            str_payload = (
                "regions=76,79,64,86,83,75,4,38,30,33,70,71,22,31,66,68,1,48,82,40,69,80"
                "124731,121709,120762,159402,2737,130744,117986,1733,686,132043"
                "&couponsGeo=12,3,18,15,21&dest=-1029256,-102269,-226149,-446116"
            )

            response = session.get(endpoint + str_payload + vectors_url, params=payload)
            response.raise_for_status()
            j_data = response.json()

            total_items = j_data["data"].get("total")

            return f'Тотал запроса {query} = {total_items}', int(total_items)


def get_merger_page(page: int, query: str):
    with requests.session() as session:
        session.headers["User-Agent"] = "insomnia/2022.2.1"
        session.mount("http://", HTTPAdapter(max_retries=retries))

        endpoint = "https://search.wb.ru/merger/catalog?"
        vectors = tokenizer(query)
        if vectors is not None:
            vectors_url = "&" + "&".join(vectors)

            payload = {
                "spp": 0,
                "pricemarginCoeff": 1.0,
                "reg": 1,
                "appType": 55,
                "offlineBonus": 0,
                "onlineBonus": 0,
                "emp": 1,
                "locale": "ru",
                "lang": "ru",
                "curr": "rub",
                "page": page,
                "dest": ["-1029256,-102269,-1278703,-1255563"],
            }
            response = session.get(endpoint + vectors_url, params=payload)
            response.raise_for_status()
            j_data = response.json()

            items = j_data["data"].get("products")

            return items
