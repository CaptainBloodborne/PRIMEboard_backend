from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Sequence
from sqlalchemy.orm import relationship

from .database import Base


def create_preset_model(token: str):

    class PresetNm(Base):
        __tablename__ = "preset_" + token

        id = Column(
            Integer,
            primary_key=True,
        )
        nm_id = Column(
            Integer,
        )

        subject_id = Column(
            Integer,
        )
        brand_id = Column(
            Integer,
        )
        stock_exists = Column(
            Boolean,
        )
        score = Column(
            Integer,
        )

    return PresetNm


# class PresetMeta(Base):
#     __tablename__ = "preset_info"
#
#     preset_id = Column(
#         String,
#         primary_key=True,
#     )
#
#     subjects = Column(
#         Sequence,
#     )
#
#     brands = Column(
#         Sequence,
#     )
#
#     in_stock = Column(
#         Integer,
#     )
#
#     total = Column(
#         Integer,
#     )
