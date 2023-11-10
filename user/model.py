import datetime
from typing import List

from pydantic import BaseModel
from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
)
from sqlalchemy.orm import Mapped, relationship
from services.db_services import Base
from todo_user.model import TodoUser


class User(Base):
    __tablename__ = "user"
    id: Mapped[int] = Column(Integer, primary_key=True)
    fullname: Mapped[str] = Column(String)
    date_birth: Mapped[datetime.date] = Column(Date)
    city: Mapped[str] = Column(String)
    todos: Mapped[List["ToDo"]] = relationship(
        secondary="todo_user", back_populates="users"
    )


class UserPydantic(BaseModel):
    fullname: str
    date_birth: datetime.date
    city: str
    todo_ids: List[int]
