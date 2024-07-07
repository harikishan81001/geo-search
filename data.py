import json

import boto3

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url='http://127.0.0.1:4566')




# Insert data into Cities Table
cities_table.put_item(
    Item={
        'city_id': '1',
        'city_name': 'CityA',
        'city_center': {'longitude': -73.935242, 'latitude': 40.730610},
        'city_bounds': json.dumps({
            'type': 'Polygon',
            'coordinates': [[...]]  # Replace with actual coordinates
        })
    }
)

# Insert data into Clusters Table
clusters_table.put_item(
    Item={
        'cluster_id': '1',
        'city_id': '1',
        'cluster_name': 'ClusterA',
        'cluster_polygon': json.dumps({
            'type': 'Polygon',
            'coordinates': [[...]]  # Replace with actual coordinates
        })
    }
)

