from services.serialization import BaseSerializer, SerializerField
from todo_slave_details.model import ToDoSlaveDetails


class ToDoSlaveDetailsSerializer(BaseSerializer):
    model = ToDoSlaveDetails
    fields = [
        SerializerField("id", "primary_key"),
        SerializerField("details", "info"),
    ]
