import graphene
from abc import abstractmethod

from . import SearchBoundingBox, PropertiesPage, PropertyFilter, Statistics
from .resolver import RESOLVER_MONGODB


class Query(graphene.ObjectType):

    properties_by_bounding_box_and_filter = graphene.Field(
        PropertiesPage,
        bounding_box=graphene.Argument(SearchBoundingBox, required=True),
        filter=graphene.Argument(PropertyFilter, required=True),
        page=graphene.Argument(graphene.String, required=False, default_value='')
    )

    statistics_by_filter = graphene.Field(
        Statistics,
        filter=graphene.Argument(PropertyFilter, required=True)
    )

    @abstractmethod
    def resolve_properties_by_bounding_box_and_filter(
            self, info,
            bounding_box: SearchBoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        pass

    @abstractmethod
    def resolve_statistics_by_filter(
            self, info,
            filter: PropertyFilter
    ) -> Statistics:

        pass


class MongoDBQuery(Query):

    def resolve_properties_by_bounding_box_and_filter(
            self, info,
            bounding_box: SearchBoundingBox,
            filter: PropertyFilter,
            page: graphene.String
    ) -> PropertiesPage:

        return RESOLVER_MONGODB.find_properties_by_bounding_box_and_filter(
            bounding_box=bounding_box,
            filter=filter,
            page=page
        )

    def resolve_statistics_by_filter(
            self, info,
            filter: PropertyFilter
    ) -> Statistics:

        return RESOLVER_MONGODB.find_statistics_by_filter(
            filter=filter
        )
