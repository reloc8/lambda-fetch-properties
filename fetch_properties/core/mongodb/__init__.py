import os
from dataclasses import dataclass
from typing import AnyStr


MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DATABASE = 'timeSeriesDB'
MONGODB_COLLECTION = 'properties'


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
