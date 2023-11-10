from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from services.db_services import Base


class TodoUser(Base):
    __tablename__ = "todo_user"
    todo_id: Mapped[int] = mapped_column(ForeignKey("todo.id"), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"), primary_key=True)
    todo: Mapped["ToDo"] = relationship("ToDo")
    user: Mapped["User"] = relationship("User")
