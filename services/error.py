class RestException(Exception):
    ...


class ValidationException(RestException):
    ...


class SQLGenerationException(RestException):
    ...
