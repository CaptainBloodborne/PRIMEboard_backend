import bz2
import time

import requests

from uuid import uuid4

from fastapi import FastAPI, Depends, HTTPException, Response, Cookie
# from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from sqlalchemy import Table

from . import db_handlers, models, schemas
from .database import SessionLocal, engine

from app_backend.utils.elasticsearcher import Preset
from app_backend.utils.nm_collector import get_nms_from_shard
from app_backend.utils.online_total import get_query
from app_backend.utils.shard_config_parser import get_shard_name

app = FastAPI()


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
@app.get("/mine/")
async def mine(# response: Response,
               preset_token: str,
               workpiece: str | None = None,
               miner: str | None = None,
               db: Session = Depends(get_db),
               # token: str = Depends(get_token, use_cache=True),
               # preset_token: str | None = Cookie(default=None),
               ) -> dict:
    """
    Searcher endpoint calls miner and get preset with predefined parameters
    :param preset_token:
    :param response:
    :param token:
    :param miner:
    :param db:
    :param workpiece: configuration for mining preset
    :return: Dict with mined products
    """
    # Init preset instance
    preset = Preset()
    if miner:
        preset.run_miner(workpiece, miner)
    else:
        preset.run_miner(workpiece)
    preset.make_preset()
    preset_nm_list = preset.get_preset
    preset_subject = preset.get_subjects
    preset_brands = preset.get_brands
    preset_stock = preset.get_in_stock

    # print(preset_token)
    #
    # if preset_token is None:
    #     response.set_cookie(key="preset_token", value=token, max_age=1800)
    #     table = models.create_preset_model(token)
    #     models.Base.metadata.create_all(bind=engine)
    # else:
    #     table_exist = models.Base.metadata.tables.get("preset_" + preset_token)
    #
    #     if table_exist is None:
    #         table = models.create_preset_model(preset_token)
    #         models.Base.metadata.create_all(bind=engine)
    #     else:
    #         table = Table(
    #             "preset_" + preset_token,
    #             models.Base.metadata,
    #             autoload=True,
    #             autoload_with=engine,
    #         )
    #
    #         table.drop(engine)
    #         models.Base.metadata.reflect(engine, extend_existing=True)
    #
    #         models.Base.metadata.create_all(bind=engine)

    models.Base.metadata.reflect(engine, extend_existing=True)
    table_exist = models.Base.metadata.tables.get("preset_" + preset_token)

    print(models.Base.metadata.tables)
    print(table_exist)
    if table_exist is None:
        table = models.create_preset_model(preset_token)
        models.Base.metadata.create_all(bind=engine)
    else:
        table = Table(
            "preset_" + preset_token,
            models.Base.metadata,
            autoload=True,
            autoload_with=engine,
        )

        table.drop(engine)
        models.Base.metadata.reflect(engine, extend_existing=True)

        models.Base.metadata.create_all(bind=engine)

    db_preset = db_handlers.create_preset(db, preset_nm_list, table)
    # if db_preset:
    #     raise HTTPException(status_code=400, detail="Preset already exists")

    print(preset_nm_list)

    return {
        "preset_total": preset.get_preset_total,
        "preset": preset_nm_list,
        "subjects": preset_subject,
        "brands": preset_brands,
        "in_stock": preset_stock,
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
        preset_token: str,
        page: int = 1,
        offset: int = 100,
        db: Session = Depends(get_db),
        # preset_token: str | None = Cookie(default=None),
) -> dict:
    """
    Take required products from db
    :param offset:
    :param preset_token:
    :param db:
    :param page: page number
    :return:
    """
    table = models.Base.metadata.tables["preset_" + preset_token]

    nms = db_handlers.get_page(db, page, table)

    return {
        "products": nms,
    }


@app.get("/nm/{nm_id}")
async def get_nm(nm_id: int, db: Session = Depends(get_db), token: str = Depends(get_token, use_cache=True)) -> dict:
    """
    Get one product
    :param token:
    :param db:
    :param nm_id: product id
    :return:
    """
    table = models.Base.metadata.tables["preset_" + preset_token]

    nm = db_handlers.get_nm(db, nm_id, token)

    return nm


# API routes
@app.get("/get_from_shard/<shard>/<subject>")
def get_from_shard(shard, subject):
    """
    Used to find nm in subject. Will be deprecated soon
    :param shard:
    :param subject:
    :return:
    """
    dict_of_nms = get_nms_from_shard(shard, subject)

    return dict_of_nms


# @app.get("/get_from_preset/<bucket>/<preset>")
# def get_from_preset(bucket, preset):
#     dict_of_nms = get_nms_from_preset(bucket, preset)
#
#     return dict_of_nms


@app.get("/get_online_total")
def get_online_total(query):

    online_search_total = get_query(query)

    return online_search_total


@app.get("/dimension_syn")
def dimension_syn(query: str) -> str:
    """
    Transform dimensions to required forms for synonyms
    :param query: human query with dimensions
    :return:
    """
    synonyms = {
        query: [
            "x".join(query.split("х")),
            " на ".join(query.split("х")),
            "*".join(query.split("х")),
            " x ".join(query.split("х")),
            " х ".join(query.split("х")),
            " * ".join(query.split("х")),
        ],
    }
    return f"{query};" + "|".join(synonyms[query]) + ";"


@app.get("/get_shards")
def get_shards(subject: str) -> dict:
    """
    Get list of shards where subject exists
    :param subject: subject id
    :return:
    """

    output = get_shard_name(subject)

    return output


# @app.get("/get_shards/list")
# def get_shards_list():
#     configs_dict_raw = bz2.decompress(redis_client_config.get("shard-configs"))
#
#     configs_dict_decoded = configs_dict_raw.decode("utf-8")
#     configs_dict = json.loads(configs_dict_decoded)
#
#     return configs_dict


# @app.get("/get_subject_name")
# def get_subject_name(subject: int):
#     subject = redis_client_config.json().get(f"subject:{subject}")
#
#     return subject.get("name")
#
#
# @app.get("/autocomplete")
# def autocomplete(term: str) -> dict:
#     if term:
#         sug_list = redis_client_config.ft().sugget("auto_subjects", term)
#         str_list = [str(sug) for sug in sug_list]
#
#         return {
#             "tips": str_list
#         }
#     else:
#         return {'error': 'missing data..'}
