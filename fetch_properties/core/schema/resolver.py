import bson
import graphene
import os
import pymongo
from abc import ABC, abstractmethod
from dataclasses import dataclass

from . import BoundingBox, PropertyFilter, PropertiesPage
from ..mapper import PropertyMapper, PropertiesPageMapper
from ..mongodb import MongoDBConnection, MONGODB_CONNECTION


MONGODB_CLIENT = pymongo.MongoClient(MONGODB_CONNECTION.uri)

MAX_PAGE_SIZE = int(os.getenv('MONGODB_MAX_PAGE_SIZE'))


@dataclass
class Resolver(ABC):

    max_page_size: int

    @abstractmethod
    def find_properties_by_bounding_box_and_filter(
            self,
            bounding_box: BoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        pass


@dataclass
class MongoDBResolver(Resolver):

    def __init__(self, max_page_size: int, mongodb_client: pymongo.MongoClient, mongodb_connection: MongoDBConnection):

        super().__init__(max_page_size=max_page_size)
        self.mongodb_client = mongodb_client
        self.mongodb_connection = mongodb_connection

    def find_properties_by_bounding_box_and_filter(
            self,
            bounding_box: BoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        database = self.mongodb_connection.database
        collection = self.mongodb_connection.collection
        page = '000000000000000000000000' if str(page).strip() == '' else page
        results = self.mongodb_client[database][collection].aggregate([
            {
                "$match": {
                    "location.point": {
                        "$geoWithin": {
                            "$box": [
                                [bounding_box.bottom_left.longitude, bounding_box.bottom_left.latitude],
                                [bounding_box.top_right.longitude, bounding_box.top_right.latitude]
                            ]
                        }
                    },
                    "n_rooms": {
                        "$gte": filter.n_rooms.min,
                        "$lte": filter.n_rooms.max
                    },
                    "surface": {
                        "$gte": filter.surface.min,
                        "$lte": filter.surface.max
                    },
                    "condition": filter.condition,
                    "cursor": {
                        "$gt": bson.ObjectId(oid=page)
                    }
                }
            },
            {"$sort": {"published_on": -1}},
            {
                "$project": {
                    "cursor": 1,
                    "price": 1,
                    "location": 1
                }
            },
            {"$limit": self.max_page_size}
        ])

        properties = []
        for result in results:
            properties.append(PropertyMapper.map(property=result))

        properties_page = PropertiesPageMapper.map(properties=properties)

        return properties_page


RESOLVER_MONGODB = MongoDBResolver(
    mongodb_client=MONGODB_CLIENT,
    mongodb_connection=MONGODB_CONNECTION,
    max_page_size=MAX_PAGE_SIZE
)
