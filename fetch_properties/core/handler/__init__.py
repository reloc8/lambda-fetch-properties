import json
import logging
from dataclasses import dataclass
from typing import Any, AnyStr, Dict

from lambda_handler import LambdaHandler
from .. import FetchPropertiesLambdaCore
from ..mongodb import MongoDBConnection, MONGODB_CONNECTION


@dataclass(init=False)
class FetchPropertiesLambda(LambdaHandler):

    logger: logging.Logger
    mongodb_connection: MongoDBConnection

    def __init__(self, logger, mongodb_connection):

        super().__init__(logger=logger)
        self.mongodb_connection = mongodb_connection
        self.core = FetchPropertiesLambdaCore(
            logger=self.logger,
            mongodb_connection=mongodb_connection
        )

    def run(self, event: Any, context: Any) -> Dict[AnyStr, Any]:

        query = event['queryStringParameters'].get('query')
        response_body = self.core.query(query=query)

        return dict(
            statusCode=200,
            body=json.dumps(response_body),
            isBase64Encoded=False
        )


LAMBDA_HANDLER = FetchPropertiesLambda(
    logger=logging.getLogger(),
    mongodb_connection=MONGODB_CONNECTION
)
