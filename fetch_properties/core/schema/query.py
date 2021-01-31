import graphene
from abc import abstractmethod

from . import BoundingBox, PropertiesPage, PropertyFilter
from .resolver import RESOLVER_MONGODB


class Query(graphene.ObjectType):

    properties_by_bounding_box_and_filter = graphene.Field(
        PropertiesPage,
        bounding_box=graphene.Argument(BoundingBox, required=True),
        filter=graphene.Argument(PropertyFilter, required=True),
        page=graphene.Argument(graphene.String, required=False, default_value='')
    )

    @abstractmethod
    def resolve_properties_by_bounding_box_and_filter(
            self, info,
            bounding_box: BoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        pass


class MongoDBQuery(Query):

    def resolve_properties_by_bounding_box_and_filter(
            self, info,
            bounding_box: BoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        return RESOLVER_MONGODB.find_properties_by_bounding_box_and_filter(
            bounding_box=bounding_box,
            filter=filter,
            page=page
        )
