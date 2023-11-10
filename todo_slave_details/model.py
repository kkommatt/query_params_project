import datetime
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import Mapped, relationship
from services.db_services import Base
from pydantic import BaseModel


class ToDoSlaveDetails(Base):
    __tablename__ = "todoslavedetails"
    id: Mapped[int] = Column(Integer, primary_key=True)
    details: Mapped[str] = Column(String, nullable=True, default=None)

    todo_slave_id = Column(Integer, ForeignKey("todoslave.id"))
    todo_slave = relationship("ToDoSlave", back_populates="slavedetails")


class ToDoSlaveDetailsPydantic(BaseModel):
    details: str | None = None
    todo_slave_id: int | None = None
