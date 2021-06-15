import bson
import graphene
import os
import pygeohash
import pymongo
from abc import ABC, abstractmethod
from dataclasses import dataclass

from . import SearchBoundingBox, PropertyFilter, PropertiesPage, Statistics, LocationBoundingBox, Point
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

        global_results = list(global_results)

        min_price = None
        max_price = None
        avg_price = None
        if len(global_results) > 0:
            global_result = global_results[0]
            min_price = global_result.get('price').get('min')
            max_price = global_result.get('price').get('max')
            avg_price = global_result.get('price').get('avg')

        global_price_statistics = PriceStatisticsMapper.map(
            min_price=min_price,
            max_price=max_price,
            avg_price=avg_price
        )
        global_statistics = GlobalStatisticsMapper.map(price_statistics=global_price_statistics)

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
                geohash=geohash,
                bounding_box=self.get_bounding_box(geohash),
                score=self.get_score(result.get('price').get('avg'), avg_price)
            ))

        result = StatisticsMapper.map(local_statistics=local_statistics, global_statistics=global_statistics)

        return result

    @staticmethod
    def get_bounding_box(geohash) -> LocationBoundingBox:

        bounding_box = LocationBoundingBox()
        bounding_box.top_right = Point()
        bounding_box.bottom_left = Point()

        if geohash is not None:

            decoded = pygeohash.decode_exactly(geohash)
            center_latitude = decoded[0]
            center_longitude = decoded[1]
            epsilon_latitude = decoded[2]
            epsilon_longitude = decoded[3]

            bounding_box.top_right.latitude = center_latitude + epsilon_latitude
            bounding_box.top_right.longitude = center_longitude + epsilon_longitude

            bounding_box.bottom_left.latitude = center_latitude - epsilon_latitude
            bounding_box.bottom_left.longitude = center_longitude - epsilon_longitude

        return bounding_box

    @staticmethod
    def get_score(local_avg, global_avg):

        score = 0
        if local_avg is not None and global_avg is not None:
            if global_avg * 0.8 <= local_avg <= global_avg * 1.2:
                score = 50
            elif local_avg > global_avg:
                score = 0
            else:
                score = 100

        return score


RESOLVER_MONGODB = MongoDBResolver(
    mongodb_client=MONGODB_CLIENT,
    mongodb_connection=MONGODB_CONNECTION,
    max_page_size=MAX_PAGE_SIZE
)
