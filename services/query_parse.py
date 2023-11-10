from typing import Type

from sqlalchemy import asc, desc, and_, select, func, case
from sqlalchemy.orm import RelationshipDirection

from services.error import SQLGenerationException
from services.query_parser import (
    SortOrder,
    ActionTree,
    NestedField,
    FilterAction,
    SortAction,
)
from services.serialization import BaseSerializer, get_prop_serializer, get_serializer

EXCLUDE_COLUMN_PREFIX = "!"

WILDCARD = "*"


def _debug_query(q):
    from sqlalchemy.dialects import sqlite

    print(q.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))


def _resolve_relationships(
    action: ActionTree, serializer: Type[BaseSerializer], id_field
):
    _model_inspect = serializer.get_model_inspection()
    _fields = []
    _joins = []
    for relation_name, relation_action_tree in action.relations.items():
        relation_src_name = serializer.get_serializer_field(relation_name)
        sql_relation = _model_inspect.relationships[relation_src_name.field]
        rel_serializer = get_prop_serializer(serializer.model, relation_src_name.field)
        _rel_cte = _relation_select(
            relation_action_tree,
            rel_serializer,
            serializer.model,
            sql_relation,
        )
        if relation_action_tree.select is not None:
            _fields.append(relation_name)
            else_case = None
            match sql_relation.direction:
                case RelationshipDirection.ONETOMANY:
                    else_case = func.json("[]")
                    agg_fn = func.json(_rel_cte.c.obj)
                case RelationshipDirection.MANYTOONE:
                    agg_fn = func.json_extract(_rel_cte.c.obj, "$[0]")
                case RelationshipDirection.MANYTOMANY:
                    else_case = func.json("[]")
                    agg_fn = func.json(_rel_cte.c.obj)
                case _:
                    raise SQLGenerationException(
                        f"Unsupported relation type: {sql_relation.direction}"
                    )
            _fields.append(
                case(
                    (_rel_cte.c.id.is_not(None), agg_fn),
                    else_=else_case,
                )
            )
        _joins.append((relation_name, _rel_cte, id_field == _rel_cte.c.id))
    return _fields, _joins


def _json_query(qo: ActionTree, serializer: Type[BaseSerializer]):
    _fields = []
    _joins = []
    _hidden_fields_to_select = []
    _exclude_fields = []
    _model_inspect = serializer.get_model_inspection()
    _wild_select = any((_field == "*" for _field in qo.select))
    _field_to_select = []
    if _wild_select:
        _field_to_select = [
            _field
            for _field in serializer.fields
            if _field.field not in _model_inspect.relationships
        ]
    else:
        _field_to_select = [
            serializer.get_serializer_field(field_alias)
            for field_alias in qo.select
            if not field_alias.startswith("!")
        ]

    for _field in qo.select:
        if _field.startswith(EXCLUDE_COLUMN_PREFIX):
            _field_to_select = [
                _field
                for _field in serializer.fields
                if _field.field not in _model_inspect.relationships
            ]
            _field_to_select.remove(serializer.get_serializer_field(_field[1:]))

    for field in _field_to_select:
        _fields.append(field.alias)
        _fields.append(serializer.get_db_field(field.field))
    if "id" not in qo.select:
        _hidden_fields_to_select.append(serializer.get_db_field("id"))
    _filters = []
    _inner_cte: list[str] = []
    for flt_item in qo.filters:
        if isinstance(flt_item.field, NestedField):
            if flt_item.field.fields[0] in qo.relations:
                rel_action = qo.relations[flt_item.field.fields[0]]
            else:
                rel_action = ActionTree()
                rel_action.select = None
                qo.relations[flt_item.field.fields[0]] = rel_action

            rel_action.filters.append(
                FilterAction(
                    field=flt_item.field.shift_down(),
                    op=flt_item.operator,
                    value=flt_item.value,
                )
            )
            _inner_cte.append(flt_item.field.fields[0])
            continue
        _filters.append(
            flt_item.operator(
                serializer.get_db_field(
                    serializer.get_serializer_field(flt_item.field).field
                ),
                flt_item.value,
            )
        )
    rel_fields, _joins = _resolve_relationships(qo, serializer, serializer.model.id)
    _fields.extend(rel_fields)

    for relation_name, relation_action_tree in qo.relations.items():
        sql_relation = _model_inspect.relationships[relation_name]

        if sql_relation.primaryjoin.left in _model_inspect.columns.values():
            this_id_col = sql_relation.primaryjoin.left
        else:
            this_id_col = sql_relation.primaryjoin.right
        has_child_id_col = this_id_col != serializer.model.id

        if has_child_id_col:
            _hidden_fields_to_select.append(this_id_col)
    obj = func.json_object(*_fields)
    q = select(obj.label("sql_rest"), *_hidden_fields_to_select)
    for join in _joins:
        match join:
            case (relation_name, cte, on_clause):
                q = q.join(
                    cte, onclause=on_clause, isouter=relation_name not in _inner_cte
                )

    if _filters:
        q = q.filter(*_filters)
    if qo.sort is not None:
        field_stack = []
        field_stack.extend(reversed(qo.sort.field.fields))
        if len(field_stack) == 1:
            q = q.order_by(
                asc(
                    serializer.get_db_field(
                        serializer.get_serializer_field(qo.sort.field.fields[0]).field
                    )
                )
                if qo.sort.order is not SortOrder.DESC
                else desc(
                    serializer.get_db_field(
                        serializer.get_serializer_field(qo.sort.field.fields[0]).field
                    )
                )
            )
        else:
            while field_stack:
                current_field = field_stack.pop()

                if current_field in qo.relations:
                    rel_action = qo.relations[current_field]
                else:
                    rel_action = ActionTree()
                    rel_action.select = None
                    qo.relations[
                        current_field
                    ] = rel_action  # TODO We must remake this, because there are some errors

                rel_action.sort = SortAction(
                    field=qo.sort.field.shift_down()
                    if isinstance(qo.sort.field, NestedField)
                    else qo.sort.field,
                    order=qo.sort.order,
                )

                if field_stack:
                    serializer_entity = get_prop_serializer(
                        serializer.model, field_stack[-1]
                    )
                else:
                    serializer_entity = get_serializer(serializer.model)

                    db_field = serializer_entity.get_db_field(
                        serializer_entity.get_serializer_field(current_field).field
                    )

                    order_by_clause = (
                        asc(db_field)
                        if qo.sort.order is not SortOrder.DESC
                        else desc(db_field)
                    )
                    q = q.order_by(order_by_clause)
                    break

        # older part of code isn`t right

        # if qo.sort.field.fields[0] in qo.relations:
        #     rel_action = qo.relations[qo.sort.field.fields[0]]
        # else:
        #     rel_action = ActionTree()
        #     rel_action.select = None
        #     qo.relations[qo.sort.field.fields[0]] = rel_action
        #
        # rel_action.sort = SortAction(
        #     field=qo.sort.field.shift_down(), order=qo.sort.order
        # )
        #
        # serializer_entity = get_serializer(
        #     _model_inspect.relationships[qo.sort.field.fields[0]].entity.entity
        # )
        # q = q.order_by(
        #     asc(
        #         serializer_entity.get_db_field(
        #             serializer_entity.get_serializer_field(
        #                 qo.sort.field.shift_down()
        #             ).field
        #         )
        #     )
        #     if qo.sort.order is not SortOrder.DESC
        #     else desc(
        #         serializer_entity.get_db_field(
        #             serializer_entity.get_serializer_field(
        #                 qo.sort.field.shift_down()
        #             ).field
        #         )
        #     )
        # )
    if qo.offset:
        q = q.offset(qo.offset)
    if qo.limit:
        q = q.limit(qo.limit)
    q = q.group_by(serializer.get_db_field("id"))
    q = q.subquery()

    return q


# REMAKE LOGIC - USE STACK, NOT RECURSION
# def _order_nested_field(
#     action: ActionTree,
#     serializer: Type[BaseSerializer],
#     q,
# ):
#     if not isinstance(action.sort.field, NestedField):
#         db_field = serializer.get_db_field(
#             serializer.get_serializer_field(action.sort.field).field
#         )
#         sort_action = (
#             asc(db_field) if action.sort.order is not SortOrder.DESC else desc(db_field)
#         )
#         q = q.order_by(sort_action)
#         rel_action = None
#     else:
#         q, rel_action = _order_nested_field(action, serializer, q)
#
#         if action.sort.field.fields[0] in action.relations:
#             action.relations[action.sort.field.fields[0]].sort = rel_action
#         else:
#             new_rel_action = ActionTree()
#             new_rel_action.select = None
#             action.relations[action.sort.field.fields[0]] = new_rel_action
#     return q, rel_action


def _relation_select(
    action: ActionTree,
    serializer: Type[BaseSerializer],
    parent_model,
    sql_relation,
):
    primaryjoin = sql_relation.primaryjoin
    fields_into_json = []  # fields that we want to select
    _joins = []  # for linked entities
    _cte = None  # Common Table Expression
    _model_inspect = (
        serializer.get_model_inspection()
    )  # Get model from serializer(mapper[Model])
    if (
        primaryjoin.left in _model_inspect.columns.values()
    ):  # Set columns id of linked entities according to type of relation
        parent_id_col = primaryjoin.left
        other_id_col = primaryjoin.right
    else:
        parent_id_col = primaryjoin.right
        other_id_col = primaryjoin.left
    has_parent_id_col = (
        parent_id_col != serializer.model.id
    )  # Check if in linked entity is foreign key for this entity
    if action.select:  # In action tree check if there are any select for parent entity
        _exclude_fields = []  # If we use "!" we store fields that we want to exclude

        _wild_select = any(
            (_field == WILDCARD for _field in action.select)
        )  # True if "*" in select fields
        _field_to_select = []
        if _wild_select:  # Get all fields that are in this serializer if "*"
            _field_to_select = [
                _field
                for _field in serializer.fields
                if _field.field
                not in _model_inspect.relationships  # We don`t add it to list if it is relation(navigation property)
            ]
        else:  # if something else than all fields *
            _field_to_select = [
                serializer.get_serializer_field(_field)
                for _field in action.select  # Take fields that entered by user
            ]
        for _field in action.select:
            if _field.startswith(EXCLUDE_COLUMN_PREFIX):  # if "!" before field
                _field_to_select = [
                    _field
                    for _field in serializer.fields
                    if _field.field not in _model_inspect.relationships
                ]
                _field_to_select.remove(
                    serializer.get_serializer_field(_field[1:])
                )  # Take all fields, but remove with !
        fld = set(
            serializer.get_db_field(field.field) for field in _field_to_select
        )  # Get fields such as in DB and provide unique
        if has_parent_id_col:
            if sql_relation.direction != RelationshipDirection.MANYTOMANY:
                fld.add(parent_id_col)  # add to set foreign key
        for flt in action.filters:
            if isinstance(
                flt.field, NestedField
            ):  # If filter to linked entity, then we don`t anything
                continue
            fld.add(
                serializer.get_db_field(
                    serializer.get_serializer_field(flt.field).field
                )  # Add to set field that we filter by
            )
        fld.add(serializer.get_db_field("id"))  # add id
        q = select(*fld)  # Create query
    else:  # if fields to select are not defined
        q = select(serializer.model)  # query for select certain model
        _field_to_select = [
            _field
            for _field in serializer.fields
            if _field.field not in _model_inspect.relationships
        ]  # There all fields are selected

    if action.sort is not None:  # if order is defined
        col = serializer.get_db_field(
            action.sort.field
        )  # field that we want rows to sort by
        col = (
            desc(col) if action.sort.order == SortOrder.DESC else asc(col)
        )  # set order ascending or descending
        q = q.order_by(col)  # Add it to query
    q = q.subquery()  # Make from query subquery to manipulate in future

    for field_def in _field_to_select or []:
        fields_into_json.append(field_def.alias)
        fields_into_json.append(
            q.c[field_def.field]
        )  # Add to list elements that we want to select

    filter_items = []
    _inner_cte: list[str] = []
    for flt_item in action.filters:
        if isinstance(
            flt_item.field, NestedField
        ):  # If we filter by field in linked entity
            if (
                flt_item.field.fields[0] in action.relations
            ):  # In [0] we have relation and in [1] certain field
                rel_action = action.relations[flt_item.field.fields[0]]
            else:
                rel_action = ActionTree()  # For linked in 2 and more depth filtering
                rel_action.select = ["id"]
                action.relations[flt_item.field.fields[0]] = rel_action

            rel_action.filters.append(
                FilterAction(
                    field=flt_item.field.shift_down(),
                    op=flt_item.operator,
                    value=flt_item.value,
                )  # add filterAction for related entities
            )
            _inner_cte.append(
                flt_item.field.fields[0]
            )  # add relation to common table expression
            continue  # while nested we do it again and again
        filter_items.append(
            flt_item.operator(
                q.c[serializer.get_serializer_field(flt_item.field).field],
                flt_item.value,
            )  # add operator that we use to filter
        )

    relation_fields_into_json, _joins = _resolve_relationships(
        action,
        serializer,
        q.c.id,
    )  # get lists that define requests parameters properly(According to relation type, etc.)
    fields_into_json.extend(relation_fields_into_json)  # fields that we want to select

    _cte = select(  # create Common Table Expression
        func.json_group_array(func.json_object(*fields_into_json)).label(
            "obj"
        ),  # JSON array comprised of all values in the aggregation
        parent_model.id.label(
            "id"
        )  # Take id from model if linked model doesn`t have foreign key
        if not has_parent_id_col
        or sql_relation.direction == RelationshipDirection.MANYTOMANY
        else q.c[parent_id_col.name].label("id"),  # Take FK if we have it
    ).select_from(q)
    # many to many check

    if sql_relation.direction == RelationshipDirection.MANYTOMANY:
        relation_model_table = (
            primaryjoin.right.table
        )  # First by id cte right table (user_id), second join by second table from right table
        _cte = _cte.join(
            relation_model_table,
            onclause=sql_relation.secondaryjoin.right == q.c.id,
            isouter=False,
        )
        _cte = _cte.join(
            parent_model,
            onclause=other_id_col == sql_relation.primaryjoin.right,
            isouter=False,
        )

    else:
        if not has_parent_id_col:  # else
            _cte = _cte.join(
                parent_model,
                onclause=other_id_col == q.c[parent_id_col.name],
                isouter=True,
            )  # Make join by other_id_col == q.c[parent_id_col.name] if linked model doesn`t have foreign key

    for relation_name, rel_cte, onclause in _joins:
        _cte = _cte.join(
            rel_cte,
            onclause=onclause,
            isouter=relation_name not in _inner_cte,
        )  # Make join in cte,is outer needed if true - left outer join
    if filter_items:
        _cte = _cte.filter(and_(*filter_items))  # Add to CTE filters if there are
    _cte = _cte.group_by(
        parent_model.id
        if not has_parent_id_col
        or sql_relation.direction == RelationshipDirection.MANYTOMANY
        else q.c[parent_id_col.name]
    )  # We group by id if there are FK else by parent_id_col.name

    return _cte.cte().prefix_with(
        "NOT MATERIALIZED"
    )  # create CTE object, and we want to first compute CTE, then use it


def get_all(query_options: ActionTree, serializer: Type[BaseSerializer]):
    query = select(
        "["
        + func.coalesce(
            func.group_concat(_json_query(query_options, serializer).c.sql_rest), ""
        )
        + "]"
    )
    return query
