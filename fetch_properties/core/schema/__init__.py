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


class Point(graphene.InputObjectType):

    latitude = graphene.Float()
    longitude = graphene.Float()


class BoundingBox(graphene.InputObjectType):

    bottom_left = graphene.InputField(Point)
    top_right = graphene.InputField(Point)


class IntRange(graphene.InputObjectType):

    min = graphene.Int()
    max = graphene.Int()


class PropertyFilter(graphene.InputObjectType):

    n_rooms = graphene.InputField(IntRange)
    surface = graphene.InputField(IntRange)
    condition = graphene.String()
