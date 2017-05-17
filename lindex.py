

from collections import namedtuple

from pyspark.sql import types as T


class Model:

    @classmethod
    def from_row(cls, row):
        return cls(**row.asDict())


class ModelMeta(type):

    def __new__(meta, name, bases, dct):
        """Generate a namedtuple from the 'schema' class attribute.
        """
        Row = namedtuple(name, dct['schema'].names)

        # By default, None for all fields.
        Row.__new__.__defaults__ = (None,) * len(Row._fields)

        bases = (Row, Model)

        return super().__new__(meta, name, bases, dct)


class ChicagoNovel(metaclass=ModelMeta):

    schema = T.StructType([
        T.StructField('book_id', T.StringType(), nullable=False),
        T.StructField('filename', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('auth_last', T.StringType()),
        T.StructField('auth_first', T.StringType()),
        T.StructField('auth_id', T.StringType()),
        T.StructField('publ_city', T.StringType()),
        T.StructField('publisher', T.StringType()),
        T.StructField('publ_date', T.IntegerType()),
        T.StructField('source', T.StringType()),
        T.StructField('nationality', T.StringType()),
        T.StructField('genre', T.StringType()),
        T.StructField('text', T.StringType()),
    ])

    def full_name(self):
        return f'{self.auth_first} {self.auth_last}'
