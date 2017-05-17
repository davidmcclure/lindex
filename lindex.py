

import csv
import re

from collections import namedtuple

from pyspark.sql import types as T


class ModelMeta(type):

    def __new__(meta, name, bases, dct):
        """Generate a namedtuple from the 'schema' class attribute.
        """
        Row = namedtuple(name, dct['schema'].names)

        # By default, None for all fields.
        Row.__new__.__defaults__ = (None,) * len(Row._fields)

        bases = (Row, Model)

        return super().__new__(meta, name, bases, dct)


class Model:

    @classmethod
    def from_rdd(cls, row):
        """Wrap a raw `Row` instance from an RDD as a model instance.

        Args:
            row (pyspark.sql.Row)

        Returns: Model
        """
        return cls(**row.asDict())


TOKEN_SCHEMA = T.StructType([
    T.StructField('token', T.StringType()),
    T.StructField('pos', T.StringType()),
])


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
        T.StructField('tokens', T.ArrayType(TOKEN_SCHEMA)),
    ])


class ChicagoAuthor(metaclass=ModelMeta):

    schema = T.StructType([
        T.StructField('auth_id', T.StringType(), nullable=False),
        T.StructField('auth_last', T.StringType()),
        T.StructField('auth_first', T.StringType()),
        T.StructField('alt_first', T.StringType()),
        T.StructField('date_b', T.IntegerType()),
        T.StructField('date_d', T.IntegerType()),
        T.StructField('nationality', T.StringType()),
        T.StructField('gender', T.StringType()),
        T.StructField('race_ethnicity', T.StringType()),
        T.StructField('hyphenated_identity', T.StringType()),
        T.StructField('sexual_identity', T.StringType()),
        T.StructField('education', T.StringType()),
        T.StructField('mfa', T.StringType()),
        T.StructField('secondary_occupation', T.StringType()),
        T.StructField('coterie', T.StringType()),
        T.StructField('religion', T.StringType()),
        T.StructField('ses', T.StringType()),
        T.StructField('geography', T.StringType()),
    ])

    @classmethod
    def from_csv(cls, path):
        """Make rows from the source CSV.

        Args:
            path (str)

        Returns: list of models
        """
        reader = csv.DictReader(open(path))

        for row in reader:

            yield cls(
                auth_id=row['AUTH_ID'],
                auth_last=row['AUTH_LAST'],
                auth_first=row['AUTH_FIRST'],
                alt_first=row['ALT_FIRST'],
                date_b=parse_year(row['DATE_B']),
                date_d=parse_year(row['DATE_D']),
                nationality=row['NATIONALITY'],
                gender=row['GENDER'],
                race_ethnicity=row['RACE_ETHNICITY'],
                hyphenated_identity=row['HYPHENATED_IDENTITY'],
                sexual_identity=row['SEXUAL_IDENTITY'],
                education=row['EDUCATION'],
                mfa=row['MFA'],
                secondary_occupation=row['SECONDARY_OCCUPATION'],
                coterie=row['COTERIE'],
                religion=row['RELIGION'],
                ses=row['CLASS'],
                geography=row['GEOGRAPHY'],
            )


def parse_year(raw):
    """Try to find a year in a string, cast to int.

    Args:
        raw (str)

    Returns: str
    """
    years = re.findall('[0-9]{4}', raw)

    if years:
        return int(years[0])
