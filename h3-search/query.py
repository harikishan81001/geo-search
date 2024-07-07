import boto3
import json
from shapely.geometry import shape, Point
import h3

from db import clusters_table 


def polygon_to_h3(polygon, resolution=9):
    """
    Convert a shapely Polygon to a set of H3 hexagons at the given resolution.
    """
    hexagons = set()
    min_lng, min_lat, max_lng, max_lat = polygon.bounds
    lat_lng_pairs = [(lat, lng) for lat in [min_lat, max_lat] for lng in [min_lng, max_lng]]
    
    for lat, lng in lat_lng_pairs:
        hexagons.update(h3.polyfill(geo_json={'type': 'Polygon', 'coordinates': [list(polygon.exterior.coords)]}, res=resolution))
    
    return hexagons

def get_clusters_in_city(city_id):
    response = clusters_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('city_id').eq(city_id)
    )
    return response['Items']

def point_to_h3(point, resolution=9):
    """
    Convert a shapely Point to an H3 hexagon at the given resolution.
    """
    return h3.geo_to_h3(point.y, point.x, resolution)

def check_point_in_clusters(point, clusters_hexagons, resolution=9):
    """
    Check if a point lies within any cluster using H3.
    """
    point_hex = point_to_h3(point, resolution)
    for cluster_id, hexagons in clusters_hexagons.items():
        if point_hex in hexagons:
            return cluster_id
    return None

# Fetch clusters from DynamoDB and index them using H3
city_id = '1'
clusters = get_clusters_in_city(city_id)
clusters_hexagons = {cluster['cluster_id']: polygon_to_h3(shape(json.loads(cluster['cluster_polygon']))) for cluster in clusters}

# Example usage
test_point = Point(-73.933242, 40.732610)
cluster_id = check_point_in_clusters(test_point, clusters_hexagons)
print("Point is in cluster:", cluster_id)

