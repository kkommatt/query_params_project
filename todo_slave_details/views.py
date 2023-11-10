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
from todo_slave.model import ToDoSlave
from todo_slave_details.serializer import ToDoSlaveDetailsSerializer
from .model import ToDoSlaveDetails, ToDoSlaveDetailsPydantic

todo_slave_details_router = APIRouter(
    prefix="/todo-slave-details",
    tags=["todo-slave-details"],
    responses={404: {"description": "Not found"}},
)


@todo_slave_details_router.get("/")
async def get_todo_slave_details(request: Request):
    query_options = parse_query(unquote(request.url.query))
    validate_query_options(query_options, ToDoSlaveDetailsSerializer)
    q = get_all(query_options, ToDoSlaveDetailsSerializer)
    return Response(content=session.scalar(q), media_type="application/json")


@todo_slave_details_router.get("/{todo_slave_id}")
async def get_todo_slave_details(todo_slave_id: int):
    return session.query(ToDoSlaveDetails).filter(todo_slave_id == ToDoSlaveDetails.todo_slave_id).all()


@todo_slave_details_router.post("/")
async def create(todo_input: ToDoSlaveDetailsPydantic):
    todo_slave_details_db = ToDoSlaveDetails(**todo_input.model_dump())
    session.add(todo_slave_details_db)
    session.commit()
    session.refresh(todo_slave_details_db)
    return todo_slave_details_db


@todo_slave_details_router.patch("/{todo_slave_details_id}")
async def update_todo_slave_details_partly(
    todo_slave_details_id: Annotated[int, Path(ge=0)], todo_slave_details_input: ToDoSlaveDetailsPydantic
):
    if not session.query(exists().where(ToDoSlaveDetails.id == todo_slave_details_id)).scalar():
        raise HTTPException(status_code=404)
    else:
        todo_slave_details_to_update = (
            session.query(ToDoSlaveDetails).filter(ToDoSlaveDetails.id == todo_slave_details_id).first()
        )
        todo_slave_details_to_update.details = todo_slave_details_input.details
        if todo_slave_details_input.todo_id:
            todo_slave_details_to_update.todo_slave_id = todo_slave_details_input.todo_slave_id
        session.commit()
        session.refresh(todo_slave_details_to_update)
        return todo_slave_details_to_update


@todo_slave_details_router.delete("/{todo_slave_details_id}", status_code=204)
def delete_todo_slave_details(todo_slave_details_id: Annotated[int, Path(ge=0)]):
    try:
        todo_slave_details_to_delete = (
            session.query(ToDoSlaveDetails).filter(todo_slave_details_id == ToDoSlaveDetails.id).one()
        )
    except NoResultFound:
        raise HTTPException(status_code=404)
    session.delete(todo_slave_details_to_delete)
    session.commit()
