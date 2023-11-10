from starlette import status
from starlette.requests import Request
from starlette.responses import JSONResponse

from services.error import ValidationException, SQLGenerationException


def validation_exception_handler(_request: Request, exc: ValidationException):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content={"message": str(exc)}
    )


def sql_exception_handler(_request: Request, exc: SQLGenerationException):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"message": str(exc)}
    )
