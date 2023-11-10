import datetime
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
from sqlalchemy.orm import Mapped, relationship
from services.db_services import Base
from pydantic import BaseModel


class ToDoSlave(Base):
    __tablename__ = "todoslave"
    id: Mapped[int] = Column(Integer, primary_key=True)
    comment: Mapped[str] = Column(String, nullable=True, default=None)
    created_at: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    todo_id: Mapped[int] = Column(ForeignKey("todo.id"))
    todo = relationship("ToDo", backref="slaves", lazy=True)
    slavedetails = relationship('ToDoSlaveDetails', uselist=False, back_populates='todo_slave')


class ToDoSlavePydantic(BaseModel):
    comment: str | None = None
    created_at: datetime.datetime
    todo_id: int | None = None
