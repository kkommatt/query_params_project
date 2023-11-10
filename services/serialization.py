from typing import Any, Type

from sqlalchemy import inspect


class SerializerField:
    def __init__(self, field: str, alias: str | None):
        self.field = field
        self.alias = alias or field


class RelationField(SerializerField):
    ...


class BaseSerializer:
    model: Any
    fields: list[SerializerField]

    @classmethod
    def get_model_inspection(cls):
        return inspect(cls.model)

    def __init_subclass__(cls, **kwargs):
        __serializers__[cls.model] = cls

    @classmethod
    def get_db_field(cls, db_field: str):
        for serializer_field in cls.fields:
            if serializer_field.field == db_field:
                return cls.model.__dict__[serializer_field.field]
        raise Exception(f"Unknown db model field {db_field}")

    @classmethod
    def get_serializer_field(cls, field_alias: str):
        for serializer_field in cls.fields:
            if serializer_field.alias == field_alias:
                return serializer_field
        raise Exception(f"Unknown serializer field {field_alias}")


__serializers__: dict[Any, Type[BaseSerializer]] = {}


def get_serializer(_type) -> Type[BaseSerializer]:
    return __serializers__.get(_type, None)


def get_prop_serializer(_type, prop: str):
    serializer = get_serializer(_type)
    _mi = serializer.get_model_inspection()
    return get_serializer(_mi.relationships[prop].entity.entity)
