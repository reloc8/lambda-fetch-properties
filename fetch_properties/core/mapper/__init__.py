from typing import Any, AnyStr, Dict, List

from ..schema import Property, PropertyLocation, PropertiesPage


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
