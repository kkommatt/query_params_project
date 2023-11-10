from services.serialization import BaseSerializer, SerializerField, RelationField
from todo.model import ToDo


class ToDoSerializer(BaseSerializer):
    model = ToDo
    fields = [
        SerializerField("id", "primary_key"),
        SerializerField("comment", "instruction"),
        SerializerField("created_at", "creation_time"),
        SerializerField("priority", "preference"),
        SerializerField("is_main", "is_principal"),
        SerializerField("worker_fullname", "worker"),
        SerializerField("due_date", "deadline"),
        SerializerField("count", "amount"),
        RelationField("slaves", "slaves"),
        RelationField("users", "users"),
    ]
