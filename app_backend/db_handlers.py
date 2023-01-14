from sqlalchemy.orm import Session
from sqlalchemy import insert

from . import models, schemas


def get_page(db: Session, page: int, table):
    """

    :param db:
    :param page:
    :return:
    """
    num = (page - 1) * 100 + 1

    if page == 1:
        num = 1

    print(table, "inside handlers")
    print(table.__dict__)
    print(table.columns)
    result = db.query(
        table
    ).filter(table.c.id >= num, table.c.id < num + 100).all()

    return result


def get_nm(db: Session, nm_id: int, token: str):
    """

    :param db:
    :param nm_id:
    :return:
    """
    return db.query(
        models.create_preset_model(token).nm_id == nm_id
    ).first()


def create_preset(db: Session, preset: list[dict], table):
    """

    :param db:
    :param preset:
    :return:
    """
    # print(table)

    db.execute(
        insert(table),
        preset,
    )

    db.commit()

    return preset
