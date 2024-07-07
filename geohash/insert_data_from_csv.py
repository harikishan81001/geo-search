import boto3
import json
import csv
from decimal import Decimal
from shapely.geometry import Polygon, Point
import geohash2
from db import dynamodb

# Initialize a session using LocalStack's DynamoDB with dummy credentials

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

# Read and insert data from CSV files
with open('ncr.csv', 'r') as cities_file:
    cities_reader = csv.DictReader(cities_file)
    for row in cities_reader:
        city_id = row['city_id']
        city_name = row['city_name']
        city_center_lat = float(row['city_center_lat'])
        city_center_lng = float(row['city_center_lng'])
        city_bounds = json.loads(row['city_bounds'])
        
        cities_table.put_item(
            Item=convert_to_decimal({
                'city_id': city_id,
                'city_name': city_name,
                'city_center': {'latitude': city_center_lat, 'longitude': city_center_lng},
                'city_bounds': {
                    'type': 'Polygon',
                    'coordinates': city_bounds
                }
            })
        )

with open('ncr-cluster.csv', 'r') as clusters_file:
    clusters_reader = csv.DictReader(clusters_file)
    for row in clusters_reader:
        cluster_id = row['cluster_id']
        city_id = row['city_id']
        cluster_name = row['cluster_name']
        cluster_polygon = json.loads(row['cluster_polygon'])
        polygon = Polygon(cluster_polygon)
        geohashes = polygon_to_geohashes(polygon)
        print(len(geohashes))
        for geohash in geohashes:
            clusters_table.put_item(
                Item=convert_to_decimal({
                    'geohash': geohash,
                    'cluster_id': cluster_id,
                    'city_id': city_id,
                    'cluster_name': cluster_name,
                    'cluster_polygon': {
                        'type': 'Polygon',
                        'coordinates': cluster_polygon
                    }
                })
            )

print("Data inserted")
