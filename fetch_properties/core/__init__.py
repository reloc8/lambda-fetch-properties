import graphene
import logging
from dataclasses import dataclass
from typing import Any, AnyStr, Dict

from .mongodb import MongoDBConnection
from .schema.query import MongoDBQuery


@dataclass
class FetchPropertiesLambdaCore:

    logger: logging.Logger
    mongodb_connection: MongoDBConnection

    @staticmethod
    def query(query: AnyStr) -> Dict[AnyStr, Any]:

        graphql_schema = graphene.Schema(query=MongoDBQuery)
        result = graphql_schema.execute(query)
        return result.to_dict()
