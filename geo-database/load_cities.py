import json
from multiprocessing import Process, Pipe
from h3 import h3
import boto3
from db import world_cities_table, cities_table_name


min_res = 2
max_res = 8

db_table = world_cities_table


class WorldCitiesDatabase():
    @staticmethod
    def query_db_table(key_condition_expression, expression_attribute_values):
        resp = world_cities_table.query(
            TableName=cities_table_name,
            KeyConditionExpression=key_condition_expression,
            ExpressionAttributeValues=expression_attribute_values)
        return resp

    def load_cities(self):
        with open('data/usa_cities.geojson', 'r') as geoj:
            cities = json.load(geoj)['features']
            _db_table = db_table
            print("Loading database table")
            with _db_table.batch_writer() as db_batch:
                for idx, city in enumerate(cities):
                    city_id = str(idx)
                    if city['properties']['NAME']:
                        hexagons = h3.polyfill(city['geometry'], max_res, geo_json_conformant=True)
                        if len(hexagons) > 0:
                            for hex in hexagons:
                                parents = [h3.h3_to_parent(hex, x) for x in range(min_res, max_res+1)]
                                range_key = "#".join(parents)+"#{}".format(city_id)
                                db_item = {
                                    'ParentCell': "{}".format(h3.h3_get_base_cell(hex)),
                                    'CellLocationIndex': range_key,
                                    'CityName': city['properties']['NAME'],
                                    'CityID': city_id
                                }
                                db_batch.put_item(Item=db_item)
                    if idx % 1000 == 0:
                        print("Processed {} cities".format(idx))

    @staticmethod
    def query_cell(h3_address):
        cities = []
        resolution = h3.h3_get_resolution(h3_address)
        base_cell = str(h3.h3_get_base_cell(h3_address))

        if resolution < max_res:
            max_query_res = resolution
        else:
            max_query_res = max_res

        range_query = "#".join([h3.h3_to_parent(h3_address, x) for x in range(min_res, max_query_res + 1)])
        key_condition_expression = "ParentCell = :parentcell AND begins_with(CellLocationIndex, :index)"
        expression_values = {
            ":parentcell": {"S": base_cell},
            ":index": {"S": range_query}
        }
        resp = WorldCitiesDatabase.query_db_table(key_condition_expression, expression_values)
        for item in resp['Items']:
            city = item['CityName']['S']
            if city not in cities:
                cities.append(city)
        return cities

    @staticmethod
    def query_polygon(geometry, resolution, compact):
        hexagons = h3.polyfill(geometry, resolution, geo_json_conformant=True)
        if compact:
            hexagons = h3.compact(hexagons)

        processes = []
        parent_connections = []

        for item in hexagons:
            parent_conn, child_conn = Pipe()
            parent_connections.append(parent_conn)

            process = Process(target=WorldCitiesDatabase.query_cell_wrapper, args=(item, child_conn))
            processes.append(process)

        for process in processes:
            process.start()

        for process in processes:
            process.join()

        cities = []
        for parent_connection in parent_connections:
            for city in parent_connection.recv()[0]:
                if city not in cities:
                    cities.append(city)

        resp = {
            'statusCode': 200,
            'body': json.dumps({'cities': cities})}
        return resp

    def query_cell_wrapper(self, h3_address, conn):
        cities = self.functions['query_cell'].invoke({'h3_address': h3_address})
        conn.send([cities])
        conn.close()