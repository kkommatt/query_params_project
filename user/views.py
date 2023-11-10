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
from todo_user.model import TodoUser
from user.serializer import UserSerializer
from user.model import User, UserPydantic

user_router = APIRouter(
    prefix="/user",
    tags=["user"],
    responses={404: {"description": "Not found"}},
)


@user_router.get("/")
async def get_users(request: Request):
    query_options = parse_query(unquote(request.url.query))
    validate_query_options(query_options, UserSerializer)
    q = get_all(query_options, UserSerializer)
    return Response(content=session.scalar(q), media_type="application/json")


@user_router.get("/{user_id}")
async def get_user(user_id: int):
    return session.query(User).filter(user_id == User.id).all()


@user_router.post("/")
async def create(user_input: UserPydantic):
    todo_ids = user_input.todo_ids
    del user_input.todo_ids
    user_db = User(**user_input.model_dump())
    session.add(user_db)
    session.commit()
    for _todo_id in todo_ids:
        todo_user_db = TodoUser(todo_id=_todo_id, user_id=user_db.id)
        session.add(todo_user_db)
        session.commit()
    session.refresh(user_db)
    return user_db


@user_router.patch("/{user_id}")
async def update_user_partly(
    user_id: Annotated[int, Path(ge=0)],
    user_input: UserPydantic,
):
    if not session.query(exists().where(User.id == user_id)).scalar():
        raise HTTPException(status_code=404)
    else:
        user_update = session.query(User).filter(User.id == user_id).first()
        user_update.fullname = user_input.fullname
        user_update.date_birth = user_input.date_birth
        user_update.city = user_input.city
        if user_input.todos:
            user_update.todos = user_input.todos
        session.commit()
        session.refresh(user_update)
        return user_update


@user_router.delete("/{user_id}", status_code=204)
def delete_user(user_id: Annotated[int, Path(ge=0)]):
    try:
        user_delete = session.query(User).filter(user_id == User.id).one()
    except NoResultFound:
        raise HTTPException(status_code=404)
    session.delete(user_delete)
    session.commit()
