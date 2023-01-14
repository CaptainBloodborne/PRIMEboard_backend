import requests

from uuid import uuid4

from fastapi import FastAPI, Depends, HTTPException, Response, Cookie
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from . import db_handlers, models, schemas
from .database import SessionLocal, engine

from app_backend.utils.elasticsearcher import Preset
from app_backend.utils.nm_collector import get_nms_from_shard
from app_backend.utils.online_total import get_query, get_merger_page
from app_backend.utils.shard_config_parser import get_shard_name

# models.Base.metadata.create_all(bind=engine)

app = FastAPI()


# @app.get("/")
# async def index(test_cookie: str | None = Cookie(default=None)):
#     return {"test_cookie": test_cookie}
#
#
# @app.get("/add_cookie")
# async def add_cookie(response: Response):
#     response.set_cookie(key="test_cookie", value="test")


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_token():
    token = uuid4()
    return str(token)


# Call searcher for mining preset
@app.get("/mine/{workpiece}")
async def mine(workpiece: str,
               response: Response,
               miner: str | None = None,
               db: Session = Depends(get_db),
               token: str = Depends(get_token),
               preset_token: str | None = Cookie(default=None),
               ) -> dict:
    """
    Searcher endpoint calls miner and get preset with predefined parameters
    :param token:
    :param miner:
    :param db:
    :param workpiece: configuartion for mining preset
    :return: Dict with mined products
    """
    preset = Preset()

    if miner:
        preset.run_miner(workpiece, miner)
    else:
        preset.run_miner(workpiece)

    preset.make_preset()
    preset_nm_list = preset.get_preset

    print("Token before setting: ", preset_token)

    if preset_token is None:
        response.set_cookie(key="preset_token", value=token)
        table = models.create_preset_model(token)
        models.Base.metadata.create_all(bind=engine)
    else:
        table = models.Base.metadata.tables["preset_" + preset_token]
        table.drop(engine)
        table = models.create_preset_model(token)
        models.Base.metadata.create_all(bind=engine)

    print(models.Base.metadata.tables)

    db_preset = db_handlers.create_preset(db, preset_nm_list, table)
    # if db_preset:
    #     raise HTTPException(status_code=400, detail="Preset already exists")

    return {
        "preset_total": preset.get_preset_total,
        "preset": preset_nm_list,
    }

    # preset_table = Table(
    #     "preset",
    #     metadata_obj,
    #     Column("id", Integer, primary_key=True),
    #     Column("nm_id", Integer),
    #     Column("subject_id", Integer),
    #     Column("brand_id", Integer),
    #     Column("stock_exists", Integer),
    #     Column("score", Float),
    # )
    #
    # metadata_obj.create_all(engine)

    # with engine.connect() as conn:
    #     result = conn.execute(
    #         insert(preset_table),
    #         preset.get_preset,
    #     )
    #     conn.commit()
    #
    # print(metadata_obj.tables)

    # return {
    #     "preset_total": preset.get_preset_total,
    #     "preset": preset_nm_list,
    # }


@app.get("/api/output")
async def get_page(
        page: int,
        db: Session = Depends(get_db),
        preset_token: str | None = Cookie(default=None),) -> dict:
    """
    Take required products from db
    :param db:
    :param page: page number
    :return:
    """
    print(preset_token)
    table = models.Base.metadata.tables["preset_" + preset_token]

    print(table)

    nms = db_handlers.get_page(db, page, table)

    return {
        "products": nms,
    }
    # num = (page - 1) * 100 + 1
    #
    # if page == 1:
    #     num = 1

    # preset_table = metadata_obj.tables["preset"]
    # stmt = select(preset_table).where(preset_table.c.id >= num, preset_table.c.id < num + 100)

    # result = list()

    # with engine.connect() as conn:
    #     for row in conn.execute(stmt):
    #         print(row)
    #         result.append(row)
    # print(len(result))

    # return {"products": result}


@app.get("/nm/{nm_id}")
async def get_nm(nm_id: int, db: Session = Depends(get_db), token: str = Depends(get_token, use_cache=True)) -> dict:
    """
    Get one product
    :param db:
    :param nm_id: product id
    :return:
    """
    nm = db_handlers.get_nm(db, nm_id, token)

    return nm

    # print(nm_id)
    # print(metadata_obj.tables)
    # preset_table = metadata_obj.tables["preset"]
    # stmt = select(preset_table).where(preset_table.c.nm_id == nm_id)
    #
    # result = list()
    #
    # with engine.connect() as conn:
    #     for row in conn.execute(stmt):
    #         print(row)
    #         result.append(row)
    #
    # return {"products": result}

# API routes
# @app_backend.get("/get_from_shard/<shard>/<subject>")
# def get_from_shard(shard, subject):
#     """
#     Used to find nm in subject. Will be deprecated soon
#     :param shard:
#     :param subject:
#     :return:
#     """
#     dict_of_nms = get_nms_from_shard(shard, subject)
#
#     return dict_of_nms


# @app_backend.get("/get_from_preset/<bucket>/<preset>")
# def get_from_preset(bucket, preset):
#     dict_of_nms = get_nms_from_preset(bucket, preset)
#
#     return dict_of_nms


# @app_backend.get("/get_available_bucket")
# def get_available_buckets():
#     wbx_api = requests.get(
#         url="http://buckets-info.wbx-search.svc.k8s.wbxsearch-dp/api/v1/info",
#     )
#     wbx_api.raise_for_status()
#
#     data_wbx = wbx_api.json()
#     cash_buckets = [f"bucket-{bucket}" for bucket in range(82, 186)]
#
#     buckets_info = dict()
#     for bucket in data_wbx:
#         num_of_nms = data_wbx[bucket].get("uniq_items")
#
#         if num_of_nms is None:
#             buckets_info.update({bucket: None})
#
#         if num_of_nms < 1_000_000 and bucket not in cash_buckets:
#             buckets_info.update({bucket: data_wbx[bucket].get("uniq_items")})
#
#     return buckets_info
#
#
# @app_backend.get("/get_online_total")
# def get_online_total(query):
#
#     online_search_total = get_query(query)
#
#     return online_search_total
#
#
# @app_backend.get("/dimension_syn")
# def dimension_syn(query: str) -> str:
#     """
#     Transform dimensions to required forms for synonyms
#     :param query: human query with dimensions
#     :return:
#     """
#     synonyms = {
#         query: [
#             "x".join(query.split("х")),
#             " на ".join(query.split("х")),
#             "*".join(query.split("х")),
#             " x ".join(query.split("х")),
#             " х ".join(query.split("х")),
#             " * ".join(query.split("х")),
#         ],
#     }
#     return f"{query};" + "|".join(synonyms[query]) + ";"
#
#
# @app_backend.get("/get_shards")
# def get_shards(subject: str) -> dict:
#     """
#     Get list of shards where subject exists
#     :param subject: subject id
#     :return:
#     """
#
#     output = get_shard_name(subject)
#
#     return output
#
#
# @app_backend.get("/get_shards/list")
# def get_shards_list():
#     configs_dict_raw = bz2.decompress(redis_client_config.get("shard-configs"))
#
#     configs_dict_decoded = configs_dict_raw.decode("utf-8")
#     configs_dict = json.loads(configs_dict_decoded)
#
#     return configs_dict
#
#
# @app_backend.get("/get_subject_name")
# def get_subject_name(subject: int):
#     subject = redis_client_config.json().get(f"subject:{subject}")
#
#     return subject.get("name")
#
#
# @app_backend.get("/autocomplete")
# def autocomplete():
#     term = request.args.get("q")
#
#     if term:
#         sug_list = redis_client_config.ft().sugget("auto_subjects", term)
#         str_list = [str(sug) for sug in sug_list]
#
#         return {
#             "tips": str_list
#         }
#     else:
#         return {'error': 'missing data..'}
