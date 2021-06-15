from typing import Any, AnyStr, Dict, List

from ..schema import Property, PropertyLocation, PropertiesPage, LocalStatistics, GlobalStatistics, Statistics, \
    PriceStatistics, LocationBoundingBox


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
    def map(price_statistics: PriceStatistics, geohash: AnyStr, bounding_box: LocationBoundingBox, score) -> LocalStatistics:

        mapped = LocalStatistics()
        mapped.price = price_statistics
        mapped.geohash = geohash
        mapped.bounding_box = bounding_box
        mapped.score = score

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
