import boto3
import json
from shapely.geometry import shape, Point, Polygon
import geohash2
from decimal import Decimal
from db import dynamodb

# Initialize a session using LocalStack's DynamoDB with dummy credentials

clusters_table = dynamodb.Table('Clusters')

def get_clusters_by_geohash(geohash):
    response = clusters_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('geohash').eq(geohash)
    )
    return response['Items']

def convert_to_float(value):
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, list):
        return [convert_to_float(v) for v in value]
    elif isinstance(value, dict):
        return {k: convert_to_float(v) for k, v in value.items()}
    return value

def check_point_in_clusters(point, precision=7):
    """
    Check if a point lies within any cluster using Geohash.
    """
    geohash = geohash2.encode(point.y, point.x, precision=precision)
    clusters = get_clusters_by_geohash(geohash)
    print(clusters)    
    for cluster in clusters:
        polygon_coordinates = convert_to_float(cluster['cluster_polygon']['coordinates'])

        polygon = Polygon(polygon_coordinates)
        if polygon.contains(point):
            return cluster['cluster_name']
    
    return None

# Test points
test_points = [
    {'latitude': 28.600792101087805, 'longitude': 77.04312829132186, 'description': 'Inside Dwarka'},
    {'latitude': 28.55103964613204, 'longitude': 76.97192393485614, 'description': 'Outside Delhi NCR'}
]

for point in test_points:
    test_point = Point(point['longitude'], point['latitude'])
    cluster_name = check_point_in_clusters(test_point)
    result = f"Point ({point['latitude']}, {point['longitude']}) is in cluster: {cluster_name}" if cluster_name else f"Point ({point['latitude']}, {point['longitude']}) is outside any cluster"
    print(f"{point['description']}: {result}")
