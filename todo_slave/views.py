from typing import Annotated
from urllib.parse import unquote

from fastapi import APIRouter, HTTPException, Path, Request
from sqlalchemy.exc import NoResultFound
from sqlalchemy.sql import exists
from starlette.responses import Response

from services.db_services import session
from services.query_parse import get_all
from services.query_validation import validate_query_options
from services.query_parser import parse_query
from todo.model import ToDo
from todo_slave.serializer import ToDoSlaveSerializer
from .model import ToDoSlave, ToDoSlavePydantic

todo_slave_router = APIRouter(
    prefix="/todo-slave",
    tags=["todo-slave"],
    responses={404: {"description": "Not found"}},
)


@todo_slave_router.get("/")
async def get_todo_slaves(request: Request):
    query_options = parse_query(unquote(request.url.query))
    validate_query_options(query_options, ToDoSlaveSerializer)
    q = get_all(query_options, ToDoSlaveSerializer)
    return Response(content=session.scalar(q), media_type="application/json")


@todo_slave_router.get("/{todo_id}")
async def get_todo_slave(todo_id: int):
    return session.query(ToDoSlave).filter(todo_id == ToDoSlave.todo_id).all()


@todo_slave_router.post("/")
async def create(todo_input: ToDoSlavePydantic):
    todo_slave_db = ToDoSlave(**todo_input.model_dump())
    session.add(todo_slave_db)
    session.commit()
    session.refresh(todo_slave_db)
    return todo_slave_db


@todo_slave_router.patch("/{todo_slave_id}")
async def update_todo_slave_partly(
    todo_slave_id: Annotated[int, Path(ge=0)], todo_slave_input: ToDoSlavePydantic
):
    if not session.query(exists().where(ToDoSlave.id == todo_slave_id)).scalar():
        raise HTTPException(status_code=404)
    else:
        todo_slave_to_update = (
            session.query(ToDoSlave).filter(ToDo.id == todo_slave_id).first()
        )
        todo_slave_to_update.comment = todo_slave_input.comment
        todo_slave_to_update.created_at = todo_slave_input.created_at
        if todo_slave_input.todo_id:
            todo_slave_to_update.todo_id = todo_slave_input
        session.commit()
        session.refresh(todo_slave_to_update)
        return todo_slave_to_update


@todo_slave_router.delete("/{todo_slave_id}", status_code=204)
def delete_todo_slave(todo_slave_id: Annotated[int, Path(ge=0)]):
    try:
        todo_slave_to_delete = (
            session.query(ToDoSlave).filter(todo_slave_id == ToDoSlave.id).one()
        )
    except NoResultFound:
        raise HTTPException(status_code=404)
    session.delete(todo_slave_to_delete)
    session.commit()
