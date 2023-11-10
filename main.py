import uvicorn
from fastapi import FastAPI

from services.db_services import Base, engine
from todo.views import todo_router
from todo_slave.views import todo_slave_router
from todo_slave_details.views import todo_slave_details_router
from exc_handlers import (
    ValidationException,
    SQLGenerationException,
    validation_exception_handler,
    sql_exception_handler,
)
from user.views import user_router

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    Base.metadata.create_all(bind=engine)


app.include_router(todo_router)
app.include_router(todo_slave_router)
app.include_router(todo_slave_details_router)
app.include_router(user_router)

app.add_exception_handler(ValidationException, validation_exception_handler)
app.add_exception_handler(SQLGenerationException, sql_exception_handler)

if __name__ == "__main__":
    uvicorn.run(app)
    # from sqlalchemy import select, func, case
    # from todo.model import ToDo
    # from services.db_services import session
    # from todo_slave.model import ToDoSlave
    #
    # subquery = (
    #     select(
    #         func.json_object(
    #             "id",
    #             ToDo.id,
    #             "created_at",
    #             ToDo.created_at,
    #             "slaves",
    #             case(
    #                 (
    #                     ToDoSlave.id.is_not(None),
    #                     func.json_group_array(
    #                         func.json_object(
    #                             "id", ToDoSlave.id, "comment", ToDoSlave.comment
    #                         )
    #                     ),
    #                 )
    #             ),
    #         ).label("js")
    #     )
    #     .join(target=ToDoSlave, onclause=ToDo.id == ToDoSlave.todo_id, isouter=True)
    #     .group_by(ToDo.id)
    #     .subquery()
    # )
    # q = select("[" + func.group_concat(subquery.c.js) + "]")
    #
    # print(session.execute(q).scalar())
