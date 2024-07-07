import boto3
import json
from decimal import Decimal
from shapely.geometry import Polygon, Point
import geohash2
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

# Function to convert polygon to geohashes
def polygon_to_geohashes(polygon, precision=7):
    min_lng, min_lat, max_lng, max_lat = polygon.bounds
    geohashes = set()
    
    lat_step = (max_lat - min_lat) / 100
    lng_step = (max_lng - min_lng) / 100
    
    lat = min_lat
    while lat <= max_lat:
        lng = min_lng
        while lng <= max_lng:
            point = Point(lng, lat)
            if polygon.contains(point):
                geohashes.add(geohash2.encode(lat, lng, precision=precision))
            lng += lng_step
        lat += lat_step
    
    return geohashes

# Insert data into Clusters Table
clusters = [
    {
        "cluster_id": "1",
        "city_id": "1",
        "cluster_name": "ClusterA",
        "cluster_polygon": Polygon([
            (-73.935242, 40.730610), 
            (-73.935242, 40.732610), 
            (-73.933242, 40.732610), 
            (-73.933242, 40.730610), 
            (-73.935242, 40.730610)
        ])
    },
    {
        "cluster_id": "2",
        "city_id": "1",
        "cluster_name": "ClusterB",
        "cluster_polygon": Polygon([
            (-73.933242, 40.732610), 
            (-73.933242, 40.735610), 
            (-73.931242, 40.735610), 
            (-73.931242, 40.732610), 
            (-73.933242, 40.732610)
        ])
    }
]

for cluster in clusters:
    geohashes = polygon_to_geohashes(cluster["cluster_polygon"])
    for geohash in geohashes:
        clusters_table.put_item(Item=convert_to_decimal({
            "geohash": geohash,
            "cluster_id": cluster["cluster_id"],
            "city_id": cluster["city_id"],
            "cluster_name": cluster["cluster_name"],
            "cluster_polygon": {
                'type': 'Polygon',
                'coordinates': [[-73.935242, 40.730610], [-73.935242, 40.732610], [-73.933242, 40.732610], [-73.933242, 40.730610], [-73.935242, 40.730610]]
            }
        }))

print("Data inserted")
