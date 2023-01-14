from pydantic import BaseModel


class Nm(BaseModel):
    nm_id: int
    subject_id: int
    brand_id: int
    stock_exists: bool
    score: float

    class Config:
        orm_mode = True


class Preset(BaseModel):
    preset: list[Nm]

    class Config:
        orm_mode = True
