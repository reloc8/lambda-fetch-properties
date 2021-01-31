import os
from dataclasses import dataclass
from typing import AnyStr


MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')

DEFAULT_MONGODB_DATABASE = 'timeSeriesDB'
DEFAULT_MONGODB_COLLECTION = 'properties'

MONGODB_DATABASE = MONGODB_DATABASE \
    if MONGODB_DATABASE and MONGODB_DATABASE.strip() != '' else DEFAULT_MONGODB_DATABASE
MONGODB_COLLECTION = MONGODB_COLLECTION \
    if MONGODB_COLLECTION and MONGODB_COLLECTION.strip() != '' else DEFAULT_MONGODB_COLLECTION


@dataclass
class MongoDBConnection:

    uri: AnyStr
    database: AnyStr
    collection: AnyStr


MONGODB_CONNECTION = MongoDBConnection(
    uri=MONGODB_URI,
    database=MONGODB_DATABASE,
    collection=MONGODB_COLLECTION
)
