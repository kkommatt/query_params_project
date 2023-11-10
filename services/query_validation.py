import operator
from typing import Type

from sqlalchemy.orm import InstrumentedAttribute

from services.error import ValidationException
from services.query_parser import ActionTree, NestedField
from services.serialization import BaseSerializer, get_serializer, SerializerField


def validate_query_options(qo: ActionTree, serializer: Type[BaseSerializer]):
    if qo.select is not None:
        _validate_select(qo, serializer)
    if qo.filters is not None:
        _validate_filter(qo, serializer)


def _validate_select(action: ActionTree, serializer: Type[BaseSerializer]):
    field_aliases = {f.alias: f for f in serializer.fields}
    if isinstance(action.select, list):
        for field in action.select:
            if field.startswith("!"):
                excluded_field = field[1:]
                if excluded_field not in field_aliases.keys():
                    raise ValidationException(
                        f"Unknown field to exclude: {excluded_field}"
                    )

            elif field == "*":
                continue
            else:
                if field not in field_aliases.keys():
                    raise ValidationException(f"Unknown field to select: {field}")
    model_inspection = serializer.get_model_inspection()
    for relation_name, rel_action in action.relations.items():
        if relation_name not in field_aliases.keys():
            raise ValidationException(f"Unknown relation passed: {relation_name}")
        field_def = field_aliases[relation_name]
        relation_ = model_inspection.relationships[field_def.field]
        relation_type = relation_.entity
        relation_serializer = get_serializer(relation_type.entity)
        _validate_select(rel_action, relation_serializer)


def _validate_filter(
    action: ActionTree, serializer: Type[BaseSerializer]
):  # actiontree
    model_inspect = serializer.get_model_inspection()
    field_aliases = {f.alias: f for f in serializer.fields}

    for flt_item in action.filters:
        if isinstance(flt_item.field, NestedField):
            if flt_item.field.fields[0] not in field_aliases.keys():
                raise ValidationException(
                    f"Unknown field passed: {flt_item.field.fields[0]}"
                )
            if flt_item.field.fields[0] not in model_inspect.relationships:
                raise ValidationException(
                    f"Unknown relation passed: {flt_item.field.fields[0]}"
                )
        else:
            if flt_item.field not in field_aliases.keys():
                raise ValidationException(f"Unknown field passed: {flt_item.field}")
        if flt_item.operator in [
            operator.ge,
            operator.gt,
            operator.lt,
            operator.le,
        ] and isinstance(flt_item.value, str):
            raise ValidationException(
                f"Filter value in this scope cannot be string: {flt_item.operator} and {flt_item.value}"
            )
        if flt_item.operator in [
            InstrumentedAttribute.like,
            InstrumentedAttribute.ilike,
        ] and not isinstance(flt_item.value, str):
            raise ValidationException(
                f"Value must be string: {flt_item.value} for operator: {flt_item.operator}"
            )

        if isinstance(flt_item.value, list) and operator.eq == flt_item.operator:
            raise ValidationException(
                "Equal operator doesn`t support list of values, please provide single value"
            )
        if (
            isinstance(flt_item.value, list)
            and InstrumentedAttribute.in_ == flt_item.operator
        ):
            _types = set(type(item) for item in flt_item.value)
            if len(_types) > 1:
                raise ValidationException("List must contains single type of value")
    for relation_name, rel_action in action.relations.items():
        if relation_name not in field_aliases.keys():
            raise ValidationException(f"Unknown relation passed: {relation_name}")
        field_def = field_aliases[relation_name]
        relation_ = model_inspect.relationships[field_def.field]
        relation_type = relation_.entity
        relation_serializer = get_serializer(relation_type.entity)
        _validate_filter(rel_action, relation_serializer)
