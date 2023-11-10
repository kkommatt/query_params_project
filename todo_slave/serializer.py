from services.serialization import BaseSerializer, SerializerField, RelationField
from todo_slave.model import ToDoSlave


class ToDoSlaveSerializer(BaseSerializer):
    model = ToDoSlave
    fields = [
        SerializerField("id", "primary_key"),
        SerializerField("comment", "instruction"),
        SerializerField("created_at", "creation_time"),
        RelationField("todo", "todo"),
        RelationField("slavedetails", "slavedetails"),
    ]
