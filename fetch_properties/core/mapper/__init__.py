import libgeohash
from typing import Any, AnyStr, Dict, List

from ..schema import Property, PropertyLocation, PropertiesPage, LocalStatistics, GlobalStatistics, Statistics, \
    PriceStatistics, LocationBoundingBox, Point


class PropertyMapper:

    @staticmethod
    def map(property: Dict[AnyStr, Any]) -> Property:

        cursor = property.get('cursor')
        price = property.get('price')
        location = property.get('location')

        property_location = PropertyLocation()
        property_location.latitude = location.get('point').get('coordinates')[1]
        property_location.longitude = location.get('point').get('coordinates')[0]
        property_location.geohash = location.get('geohash')

        mapped = Property()
        mapped.id = cursor
        mapped.price = price
        mapped.location = property_location

        return mapped


class PropertiesPageMapper:

    @staticmethod
    def map(properties: List[Property]) -> PropertiesPage:

        properties_page = PropertiesPage()
        properties_page.properties = properties
        properties_page.page = properties[-1].id

        return properties_page


class StatisticsMapper:

    @staticmethod
    def map(local_statistics: List[LocalStatistics], global_statistics: GlobalStatistics) -> Statistics:

        mapped = Statistics()
        mapped.local_statistics = local_statistics
        mapped.global_statistics = global_statistics

        return mapped


class LocalStatisticsMapper:

    @staticmethod
    def map(price_statistics: PriceStatistics, geohash: AnyStr, bounding_box: LocationBoundingBox) -> LocalStatistics:

        mapped = LocalStatistics()
        mapped.price = price_statistics
        mapped.geohash = geohash
        mapped.bounding_box = bounding_box

        return mapped


class GlobalStatisticsMapper:

    @staticmethod
    def map(price_statistics: PriceStatistics) -> GlobalStatistics:

        mapped = GlobalStatistics()
        mapped.price = price_statistics

        return mapped


class PriceStatisticsMapper:

    @staticmethod
    def map(min_price, max_price, avg_price) -> PriceStatistics:

        mapped = PriceStatistics()
        mapped.min = min_price
        mapped.max = max_price
        mapped.avg = avg_price

        return mapped


class LocationBoundingBoxMapper:

    @staticmethod
    def map(geohash: AnyStr) -> LocationBoundingBox:

        reversed_geohash = libgeohash.bbox(geohash)

        mapped = LocationBoundingBox()
        top_right = Point()
        top_right.latitude = reversed_geohash.get('n')
        top_right.longitude = reversed_geohash.get('e')
        mapped.top_right = top_right
        bottom_left = Point()
        bottom_left.latitude = reversed_geohash.get('s')
        bottom_left.longitude = reversed_geohash.get('w')
        mapped.bottom_left = bottom_left

        return mapped
