import boto3
import json
from decimal import Decimal
from shapely.geometry import Polygon
from db import dynamodb


cities_table = dynamodb.Table('Cities')
clusters_table = dynamodb.Table('Clusters')

# Function to convert float to Decimal
def convert_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, list):
        return [convert_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_to_decimal(v) for k, v in obj.items()}
    return obj

# Insert data into Cities Table
cities_table.put_item(
    Item=convert_to_decimal({
        'city_id': '1',
        'city_name': 'CityA',
        'city_center': {'longitude': -73.935242, 'latitude': 40.730610},
        'city_bounds': {
            'type': 'Polygon',
            'coordinates': [[-73.935242, 40.730610], [-73.935242, 40.735610], [-73.930242, 40.735610], [-73.930242, 40.730610], [-73.935242, 40.730610]]
        }
    })
)

# Insert data into Clusters Table
clusters = [
    {
        "cluster_id": "1",
        "city_id": "1",
        "cluster_name": "ClusterA",
        "cluster_polygon": {
            'type': 'Polygon',
            'coordinates': [[-73.935242, 40.730610], [-73.935242, 40.732610], [-73.933242, 40.732610], [-73.933242, 40.730610], [-73.935242, 40.730610]]
        }
    },
    {
        "cluster_id": "2",
        "city_id": "1",
        "cluster_name": "ClusterB",
        "cluster_polygon": {
            'type': 'Polygon',
            'coordinates': [[-73.933242, 40.732610], [-73.933242, 40.735610], [-73.931242, 40.735610], [-73.931242, 40.732610], [-73.933242, 40.732610]]
        }
    }
]

for cluster in clusters:
    clusters_table.put_item(Item=convert_to_decimal(cluster))

print("Data inserted")




