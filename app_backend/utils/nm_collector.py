import copy
import requests
import logging
import sys
import asyncio

from aiohttp import ClientSession, ClientTimeout


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("pop_merger_sorter.py")
logging.getLogger("chardet.charsetprober").disabled = True


def fetch_total(api: str, payload: dict, param: int):

    params = copy.deepcopy(payload)
    if "presets" in api:
        params["preset"] = param
    else:
        params["subject"] = param
    total_req = requests.get(api, params=params)
    total_req.raise_for_status()
    logger.info("Got response [%s] for URL: %s", total_req.status_code, api)
    total_jeyson = total_req.json()
    total_num = total_jeyson["data"]["total"]
    logger.info(f"Total is : {total_num}")

    return total_num


async def parse(api: str, session: ClientSession, nm_list, params: dict = None):
    nm_req = params
    if nm_req:
        async with session.get(
                url=api, params=params
        ) as resp:
            resp_jeyson = await resp.json(content_type='text/plain')
            resp_list = resp_jeyson["data"]["products"]
            nm_list["products"].extend(resp_list)
    else:
        nm_req = await session.request(
            method="GET", url=api,
        )
        nm_req.raise_for_status()
        try:
            nm_list_jeyson = await nm_req.json(content_type='text/plain')
        except Exception as err:
            logger.error(f"Error occured: <{err}>")
            nm_list["products"].extend([])
        else:
            nms_list = nm_list_jeyson["data"]["products"]
            nm_list["products"].extend(nms_list)



async def chain_shard(subject: int, shard: str, params: dict, nms_list):
    total_api = f"https://catalog.wb.ru/catalog/{shard}/filters"
    total_num = fetch_total(total_api, params, subject)
    timeout = ClientTimeout(total=420)
    async with ClientSession(timeout=timeout) as session:
        tasks = []
        for page in range(1, (total_num // 100) + 2):
            api = f"https://catalog.wb.ru/catalog/{shard}/catalog?subject={subject}&page={page}&locale=ru"
            tasks.append(
                parse(api, session, nms_list)
            )

        await asyncio.gather(*tasks)


async def chain_presets(preset: int, bucket: str, payload: dict, nms_list):

    total_api = f"https://search.wb.ru/presets/{bucket}/filters"
    total_num = fetch_total(total_api, payload, preset)
    timeout = ClientTimeout(total=420)

    logging.info(f"Parametrs for req: {payload}")
    async with ClientSession(timeout=timeout) as session:
        tasks = []
        payload["preset"] = preset
        for page in range(1, (total_num // 100) + 2):
            payload["page"] = page
            api = f"https://search.wb.ru/presets/{bucket}/catalog?preset={preset}&page={page}"
            tasks.append(
                parse(api, session, nms_list, payload)
            )

        await asyncio.gather(*tasks)


def get_nms_from_shard(shard, subject):
    PARAMS = {
        "filters": "xsubject",
        "spp": 0,
        "regions": ",".join(map(str, [83, 75, 64, 4, 38, 30, 33, 70, 71, 22, 31, 66, 68, 40, 82, 48, 1, 69, 80])),
        "stores": ",".join(map(
            str, [
                117673, 122258, 122259, 125238, 125239,
                125240, 507, 3158, 117501, 120602,
                120762, 6158, 121709, 124731, 130744,
                159402, 2737, 117986, 1733, 686, 132043
            ])),
        "pricemarginCoeff": 1.0,
        "reg": 0,
        "appType": 1,
        "offlineBonus": 0,
        "onlineBonus": 0,
        "emp": 0,
        "locale": "ru",
        "lang": "ru",
        "curr": "rub",
        "couponsGeo": ",".join(map(str, [12, 3, 18, 15, 21])),
        "dest": ",".join(map(str, [-1029256, -102269, -2162196, -1257786])),
        "sort": "popular",
    }

    nms = {
        "products": []
    }

    asyncio.run(chain_shard(subject, shard, PARAMS, nms))

    return nms


def get_nms_from_preset(bucket, preset):
    PARAMS = {
        "filters": "xsubject",
        "spp": 0,
        "regions": ",".join(map(str, [64, 83, 4, 38, 33, 70, 82, 69, 86, 75, 30, 1, 40, 22, 31, 66, 48, 80, 71, 68])),
        "stores": ",".join(map(
            str, [
                117673, 122258, 122259, 125238, 125239, 125240,
                6159, 507, 3158, 117501,
                120602, 120762, 6158, 121709, 124731, 159402,
                2737, 130744, 117986, 1733, 686, 132043
            ])),
        "pricemarginCoeff": 1.0,
        "reg": 0,
        "appType": 1,
        "offlineBonus": 0,
        "onlineBonus": 0,
        "emp": 0,
        "locale": "ru",
        "lang": "ru",
        "curr": "rub",
        "couponsGeo": ",".join(map(str, [12, 3, 18, 15, 21])),
        "dest": ",".join(map(str, [-1029256, -102269, -1278703, -1255563])),
        "sort": "popular",
    }

    nms = {
        "products": []
    }

    asyncio.run(chain_presets(preset, bucket, PARAMS, nms))

    return nms
