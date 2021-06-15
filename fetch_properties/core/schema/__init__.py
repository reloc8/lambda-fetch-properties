import graphene


class PropertyLocation(graphene.ObjectType):

    latitude = graphene.Float()
    longitude = graphene.Float()
    geohash = graphene.String()


class Property(graphene.ObjectType):

    id = graphene.String()
    price = graphene.Int()
    location = graphene.Field(PropertyLocation)


class PropertiesPage(graphene.ObjectType):

    properties = graphene.List(Property)
    page = graphene.String()


class PriceStatistics(graphene.ObjectType):

    min = graphene.Int()
    max = graphene.Int()
    avg = graphene.Float()


class Point(graphene.ObjectType):

    latitude = graphene.Float()
    longitude = graphene.Float()


class LocationBoundingBox(graphene.ObjectType):

    bottom_left = graphene.Field(Point)
    top_right = graphene.Field(Point)


class LocalStatistics(graphene.ObjectType):

    geohash = graphene.String()
    price = graphene.Field(PriceStatistics)
    bounding_box = graphene.Field(LocationBoundingBox)
    score = graphene.Int()


class GlobalStatistics(graphene.ObjectType):

    price = graphene.Field(PriceStatistics)


class Statistics(graphene.ObjectType):

    local_statistics = graphene.List(LocalStatistics)
    global_statistics = graphene.Field(GlobalStatistics)


class SearchLocation(graphene.InputObjectType):

    latitude = graphene.Float()
    longitude = graphene.Float()


class SearchBoundingBox(graphene.InputObjectType):

    bottom_left = graphene.InputField(SearchLocation)
    top_right = graphene.InputField(SearchLocation)


class IntRange(graphene.InputObjectType):

    min = graphene.Int()
    max = graphene.Int()


class PropertyFilter(graphene.InputObjectType):

    n_rooms = graphene.InputField(IntRange)
    surface = graphene.InputField(IntRange)
    condition = graphene.String()
