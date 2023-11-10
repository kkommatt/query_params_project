import datetime
from typing import List

from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, func, Boolean, Date
from sqlalchemy.orm import Mapped, relationship
from services.db_services import Base
from todo_user.model import TodoUser


class ToDo(Base):
    __tablename__ = "todo"
    id: Mapped[int] = Column(Integer, primary_key=True)
    comment: Mapped[str] = Column(String, nullable=True, default=None)
    created_at: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    priority: Mapped[int] = Column(Integer)
    is_main: Mapped[bool] = Column(Boolean)
    worker_fullname: Mapped[str] = Column(String)
    due_date: Mapped[datetime.date] = Column(Date)
    count: Mapped[int] = Column(Integer, default=1)
    users: Mapped[List["User"]] = relationship(
        secondary="todo_user", back_populates="todos"
    )


class ToDoPydantic(BaseModel):
    comment: str | None = None
    created_at: datetime.datetime
    priority: int
    is_main: bool = False
    worker_fullname: str
    due_date: datetime.date
    count: int = 0
    user_ids: List[int]
