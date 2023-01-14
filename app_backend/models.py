from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .database import Base


def create_preset_model(token: str):

    class PresetNm(Base):
        __tablename__ = "preset_" + token

        id = Column(
            Integer,
            primary_key=True,
            index=True,
        )
        nm_id = Column(
            Integer,
            unique=True,
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
