"""Models for testing."""

from django.db.models import CASCADE, BooleanField, CharField, ForeignKey, IntegerField

from winipedia_django.utils.db.models import BaseModel


class ModelA(BaseModel):
    """Test model for ImportDataBaseCommand."""

    str_field: CharField[str, str] = CharField(max_length=100)
    int_field: IntegerField[int, int] = IntegerField()


class ModelB(BaseModel):
    """Test model for ImportDataBaseCommand."""

    model_a: ForeignKey[ModelA, ModelA] = ForeignKey(ModelA, on_delete=CASCADE)


class ModelC(BaseModel):
    """Test model for ImportDataBaseCommand."""

    model_b: ForeignKey[ModelB, ModelB] = ForeignKey(ModelB, on_delete=CASCADE)
    bool_field: BooleanField[bool, bool] = BooleanField()
