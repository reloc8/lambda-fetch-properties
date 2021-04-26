import bson
import json
import os
import pymongo
import unittest

from testcontainers.mongodb import MongoDbContainer


MONGODB_CONTAINER = MongoDbContainer('mongo:latest')


class TestFetchPropertiesLambda(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:

        MONGODB_CONTAINER.start()

    def testRunWhenReceiveApiGatewayEventAndQueryWithNoCursor(self):

        mongodb_uri = MONGODB_CONTAINER.get_connection_url()
        os.environ['MONGODB_URI'] = mongodb_uri
        os.environ['MONGODB_MAX_PAGE_SIZE'] = '100'
        os.environ['MONGODB_DATABASE'] = ''
        os.environ['MONGODB_COLLECTION'] = ''

        with open('resources/collection-1.json', 'r') as file:
            collection = json.load(file)

        with open('resources/event-api-gateway.json', 'r') as file:
            event = json.load(file)

        with open('resources/query-1.graphql', 'r') as file:
            query = ' '.join(file.readlines())

        with open('resources/response-body-1.json', 'r') as file:
            expected_body = json.load(file)

        with open('resources/event-api-gateway-response.json', 'r') as file:
            event_response = json.load(file)

        event['queryStringParameters'] = dict(query=query)
        expected_response = event_response
        expected_response['body'] = json.dumps(expected_body)

        from fetch_properties.core.handler import LAMBDA_HANDLER, MONGODB_CONNECTION

        mongodb_connection = MONGODB_CONNECTION
        mongodb_database = mongodb_connection.database
        mongodb_collection = mongodb_connection.collection
        mock_mongodb_client = pymongo.MongoClient(mongodb_uri)

        for document in collection:
            document['cursor'] = bson.ObjectId(oid=document['cursor'])

        mock_mongodb_client[mongodb_database][mongodb_collection].insert_many(collection)

        actual_response = LAMBDA_HANDLER.run(event=event, context=None)

        self.assertEqual('timeSeriesDB', mongodb_database)
        self.assertEqual('properties', mongodb_collection)
        self.assertDictEqual(expected_response, actual_response)

        mock_mongodb_client.drop_database(mongodb_database)

    def testRunWhenReceiveApiGatewayEventAndQueryWithCursor(self):

        mongodb_uri = MONGODB_CONTAINER.get_connection_url()
        os.environ['MONGODB_URI'] = mongodb_uri
        os.environ['MONGODB_MAX_PAGE_SIZE'] = '2'
        os.environ['MONGODB_DATABASE'] = ''
        os.environ['MONGODB_COLLECTION'] = ''

        with open('resources/collection-2.json', 'r') as file:
            collection = json.load(file)

        with open('resources/event-api-gateway.json', 'r') as file:
            event = json.load(file)

        with open('resources/query-2.graphql', 'r') as file:
            query = ' '.join(file.readlines())

        with open('resources/response-body-2.json', 'r') as file:
            expected_body = json.load(file)

        with open('resources/event-api-gateway-response.json', 'r') as file:
            event_response = json.load(file)

        event['queryStringParameters'] = dict(query=query)
        expected_response = event_response
        expected_response['body'] = json.dumps(expected_body)

        from fetch_properties.core.handler import LAMBDA_HANDLER, MONGODB_CONNECTION

        mongodb_connection = MONGODB_CONNECTION
        mongodb_database = mongodb_connection.database
        mongodb_collection = mongodb_connection.collection
        mock_mongodb_client = pymongo.MongoClient(mongodb_uri)

        for document in collection:
            document['cursor'] = bson.ObjectId(oid=document['cursor'])

        mock_mongodb_client[mongodb_database][mongodb_collection].insert_many(collection)

        actual_response = LAMBDA_HANDLER.run(event=event, context=None)

        self.assertEqual('timeSeriesDB', mongodb_database)
        self.assertEqual('properties', mongodb_collection)
        self.assertDictEqual(expected_response, actual_response)

        mock_mongodb_client.drop_database(mongodb_database)

    def testRunWhenReceiveApiGatewayEventAndQueryStatistics(self):

        mongodb_uri = MONGODB_CONTAINER.get_connection_url()
        os.environ['MONGODB_URI'] = mongodb_uri
        os.environ['MONGODB_MAX_PAGE_SIZE'] = '2'
        os.environ['MONGODB_DATABASE'] = ''
        os.environ['MONGODB_COLLECTION'] = ''

        with open('resources/collection-3.json', 'r') as file:
            collection = json.load(file)

        with open('resources/event-api-gateway.json', 'r') as file:
            event = json.load(file)

        with open('resources/query-3.graphql', 'r') as file:
            query = ' '.join(file.readlines())

        with open('resources/response-body-3.json', 'r') as file:
            expected_body = json.load(file)

        with open('resources/event-api-gateway-response.json', 'r') as file:
            event_response = json.load(file)

        event['queryStringParameters'] = dict(query=query)
        expected_response = event_response
        expected_response['body'] = json.dumps(expected_body)

        from fetch_properties.core.handler import LAMBDA_HANDLER, MONGODB_CONNECTION

        mongodb_connection = MONGODB_CONNECTION
        mongodb_database = mongodb_connection.database
        mongodb_collection = mongodb_connection.collection
        mock_mongodb_client = pymongo.MongoClient(mongodb_uri)

        for document in collection:
            document['cursor'] = bson.ObjectId(oid=document['cursor'])

        mock_mongodb_client[mongodb_database][mongodb_collection].insert_many(collection)

        actual_response = LAMBDA_HANDLER.run(event=event, context=None)

        actual_body = json.loads(actual_response['body'])
        expected_body = json.loads(expected_response['body'])

        self.assertEqual('timeSeriesDB', mongodb_database)
        self.assertEqual('properties', mongodb_collection)

        self.assertCountEqual(actual_body['data']['statisticsByFilter']['localStatistics'], expected_body['data']['statisticsByFilter']['localStatistics'])
        self.assertDictEqual(actual_body['data']['statisticsByFilter']['globalStatistics'], expected_body['data']['statisticsByFilter']['globalStatistics'])

        mock_mongodb_client.drop_database(mongodb_database)

    def testRunWhenReceiveApiGatewayEventAndQueryStatisticsAndCollectionIsEmpty(self):

        mongodb_uri = MONGODB_CONTAINER.get_connection_url()
        os.environ['MONGODB_URI'] = mongodb_uri
        os.environ['MONGODB_MAX_PAGE_SIZE'] = '2'
        os.environ['MONGODB_DATABASE'] = ''
        os.environ['MONGODB_COLLECTION'] = ''

        with open('resources/event-api-gateway.json', 'r') as file:
            event = json.load(file)

        with open('resources/query-3.graphql', 'r') as file:
            query = ' '.join(file.readlines())

        with open('resources/response-body-4.json', 'r') as file:
            expected_body = json.load(file)

        with open('resources/event-api-gateway-response.json', 'r') as file:
            event_response = json.load(file)

        event['queryStringParameters'] = dict(query=query)
        expected_response = event_response
        expected_response['body'] = json.dumps(expected_body)

        from fetch_properties.core.handler import LAMBDA_HANDLER, MONGODB_CONNECTION

        mongodb_connection = MONGODB_CONNECTION
        mongodb_database = mongodb_connection.database
        mongodb_collection = mongodb_connection.collection
        mock_mongodb_client = pymongo.MongoClient(mongodb_uri)

        actual_response = LAMBDA_HANDLER.run(event=event, context=None)

        actual_body = json.loads(actual_response['body'])
        expected_body = json.loads(expected_response['body'])

        self.assertEqual('timeSeriesDB', mongodb_database)
        self.assertEqual('properties', mongodb_collection)

        self.assertCountEqual(actual_body['data']['statisticsByFilter']['localStatistics'], expected_body['data']['statisticsByFilter']['localStatistics'])
        self.assertDictEqual(actual_body['data']['statisticsByFilter']['globalStatistics'], expected_body['data']['statisticsByFilter']['globalStatistics'])

        mock_mongodb_client.drop_database(mongodb_database)

    @classmethod
    def tearDownClass(cls) -> None:

        MONGODB_CONTAINER.stop()


if __name__ == '__main__':
    unittest.main()
