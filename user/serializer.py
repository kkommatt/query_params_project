from services.serialization import BaseSerializer, SerializerField, RelationField
from user.model import User


class UserSerializer(BaseSerializer):
    model = User
    fields = [
        SerializerField("id", "primary_key"),
        SerializerField("fullname", "fullname"),
        SerializerField("date_birth", "date_birth"),
        SerializerField("city", "city"),
        RelationField("todos", "todos"),
    ]
