from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Request
from fastapi.responses import ORJSONResponse
from sqlalchemy import exists
from sqlalchemy.exc import NoResultFound
from starlette import status
from starlette.responses import Response

from services.db_services import session
from services.query_parse import (
    get_all,
)
from services.query_validation import validate_query_options
from services.query_parser import parse_query
from todo.model import ToDo, ToDoPydantic
from todo.serializer import ToDoSerializer
from urllib.parse import unquote

from todo_user.model import TodoUser

todo_router = APIRouter(
    prefix="/todo",
    tags=["todo"],
    responses={404: {"description": "Not found"}},
)


@todo_router.get("/")
async def get_todo(request: Request):
    query_options = parse_query(unquote(request.url.query))
    validate_query_options(query_options, ToDoSerializer)
    q = get_all(query_options, ToDoSerializer)
    return Response(content=session.scalar(q), media_type="application/json")


@todo_router.get("/{todo_id}")
async def get_one(todo_id: Annotated[int, Path(ge=0)]):
    try:
        return session.query(ToDo).filter(todo_id == ToDo.id).one()
    except NoResultFound:
        raise HTTPException(status_code=404)


@todo_router.post("/")
async def create(todo_input: ToDoPydantic):
    user_ids = todo_input.user_ids
    del todo_input.user_ids
    todo_db = ToDo(**todo_input.model_dump())
    session.add(todo_db)
    session.commit()
    for _user_id in user_ids:
        todo_user_db = TodoUser(todo_id=todo_db.id, user_id=_user_id)
        session.add(todo_user_db)
        session.commit()
    session.refresh(todo_db)
    return todo_db


@todo_router.put("/{todo_id}")
async def update_todo(todo_id: Annotated[int, Path(ge=0)], todo_input: ToDoPydantic):
    if not session.query(exists().where(ToDo.id == todo_id)).scalar():
        raise HTTPException(status_code=404)
    else:
        todo_db = ToDo(**todo_input.model_dump())
        session.merge(todo_db)
        session.refresh(todo_db)
        session.commit()
        return todo_db


@todo_router.patch("/{todo_id}")
async def update_todo_partly(
    todo_id: Annotated[int, Path(ge=0)], todo_input: ToDoPydantic
):
    if not session.query(exists().where(ToDo.id == todo_id)).scalar():
        raise HTTPException(status_code=404)
    else:
        todo_to_update = session.query(ToDo).filter(ToDo.id == todo_id).first()
        todo_to_update.comment = todo_input.comment
        todo_to_update.priority = todo_input.priority
        todo_to_update.is_main = todo_input.is_main
        todo_to_update.due_date = todo_input.due_date
        todo_to_update.worker_fullname = todo_input.worker_fullname
        todo_to_update.created_at = todo_input.created_at
        session.commit()

        return todo_input


@todo_router.delete("/{todo_id}", status_code=204)
async def delete_todo(todo_id: Annotated[int, Path(ge=0)]):
    try:
        todo_to_delete = session.query(ToDo).filter(todo_id == ToDo.id).one()
    except NoResultFound:
        raise HTTPException(status_code=404)
    session.delete(todo_to_delete)
    session.commit()
