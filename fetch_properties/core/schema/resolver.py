import bson
import graphene
import os
import pymongo
from abc import ABC, abstractmethod
from dataclasses import dataclass

from . import SearchBoundingBox, PropertyFilter, PropertiesPage, Statistics
from ..mapper import PropertyMapper, PropertiesPageMapper, LocalStatisticsMapper, PriceStatisticsMapper, \
    GlobalStatisticsMapper, StatisticsMapper
from ..mongodb import MongoDBConnection, MONGODB_CONNECTION


MONGODB_CLIENT = pymongo.MongoClient(MONGODB_CONNECTION.uri)

MAX_PAGE_SIZE = int(os.getenv('MONGODB_MAX_PAGE_SIZE'))
MAX_COLLECTION_SIZE = 20_000


@dataclass
class Resolver(ABC):

    max_page_size: int

    @abstractmethod
    def find_properties_by_bounding_box_and_filter(
            self,
            bounding_box: SearchBoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        pass

    @abstractmethod
    def find_statistics_by_filter(
            self,
            filter: PropertyFilter
    ) -> Statistics:

        pass


@dataclass
class MongoDBResolver(Resolver):

    def __init__(self, max_page_size: int, mongodb_client: pymongo.MongoClient, mongodb_connection: MongoDBConnection):

        super().__init__(max_page_size=max_page_size)
        self.mongodb_client = mongodb_client
        self.mongodb_connection = mongodb_connection

    def find_properties_by_bounding_box_and_filter(
            self,
            bounding_box: SearchBoundingBox,
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

    def find_statistics_by_filter(self, filter: PropertyFilter) -> Statistics:

        database = self.mongodb_connection.database
        collection = self.mongodb_connection.collection

        geohash_precision = 7
        match_filter = {
                "$match": {
                    "n_rooms": {
                        "$gte": filter.n_rooms.min,
                        "$lte": filter.n_rooms.max
                    },
                    "surface": {
                        "$gte": filter.surface.min,
                        "$lte": filter.surface.max
                    },
                    "condition": filter.condition
                }
            }
        sort_filter = {"$sort": {"published_on": -1}}
        limit_filter = {"$limit": MAX_COLLECTION_SIZE}

        local_results = self.mongodb_client[database][collection].aggregate([
            match_filter,
            sort_filter,
            limit_filter,
            {
                "$group": {
                    "_id": {
                        "$substr": ["$location.geohash", 0, geohash_precision]
                    },
                    "price_min": {
                        "$min": "$price"
                    },
                    "price_max": {
                        "$max": "$price"
                    },
                    "price_avg": {
                        "$avg": "$price"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "geohash": "$_id",
                    "price": {
                        "min": "$price_min",
                        "max": "$price_max",
                        "avg": "$price_avg"
                    }
                }
            }
        ])

        global_results = self.mongodb_client[database][collection].aggregate([
            match_filter,
            sort_filter,
            limit_filter,
            {
                "$group": {
                    "_id": None,
                    "price_min": {
                        "$min": "$price"
                    },
                    "price_max": {
                        "$max": "$price"
                    },
                    "price_avg": {
                        "$avg": "$price"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "price": {
                        "min": "$price_min",
                        "max": "$price_max",
                        "avg": "$price_avg"
                    }
                }
            }
        ])

        local_statistics = []
        for result in local_results:
            price_statistics = PriceStatisticsMapper.map(
                min_price=result.get('price').get('min'),
                max_price=result.get('price').get('max'),
                avg_price=result.get('price').get('avg')
            )
            geohash = result.get('geohash')
            local_statistics.append(LocalStatisticsMapper.map(
                price_statistics=price_statistics,
                geohash=geohash
            ))

        global_result = list(global_results)[0]
        global_price_statistics = PriceStatisticsMapper.map(
            min_price=global_result.get('price').get('min'),
            max_price=global_result.get('price').get('max'),
            avg_price=global_result.get('price').get('avg')
        )
        global_statistics = GlobalStatisticsMapper.map(price_statistics=global_price_statistics)

        result = StatisticsMapper.map(local_statistics=local_statistics, global_statistics=global_statistics)

        return result


RESOLVER_MONGODB = MongoDBResolver(
    mongodb_client=MONGODB_CLIENT,
    mongodb_connection=MONGODB_CONNECTION,
    max_page_size=MAX_PAGE_SIZE
)
