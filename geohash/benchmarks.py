import boto3
from shapely.geometry import Point, Polygon
import geohash2
from decimal import Decimal
import time
from tqdm import tqdm
from loguru import logger
import random
import statistics
import atexit

from db import dynamodb

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
    for cluster in clusters:
        polygon_coordinates = convert_to_float(cluster['cluster_polygon']['coordinates'])
        polygon = Polygon(polygon_coordinates)
        if polygon.contains(point):
            return cluster['cluster_name']
    return None

# Generate 100,000 test points (some inside and some outside clusters)
test_points = []
for _ in range(10000):
    if random.random() > 0.5:  # 50% chance to be inside or outside
        # Inside Delhi NCR
        lat = random.uniform(28.4000, 28.8000)
        lng = random.uniform(76.8000, 77.2000)
    else:
        # Outside Delhi NCR
        lat = random.uniform(27.0000, 29.0000)
        lng = random.uniform(75.0000, 78.0000)
    test_points.append({'latitude': lat, 'longitude': lng})

# Benchmarking
times = []
points_stats = []

logger.add("benchmark.log", rotation="1 MB", backtrace=True, diagnose=True)  # Log to a file, rotating after 1 MB

def save_stats():
    match_count = len([e["cluster"] for e in points_stats if e["cluster"] != None])
    # Calculate statistics
    mean_time = statistics.mean(times) if times else 0
    median_time = statistics.median(times) if times else 0
    stdev_time = statistics.stdev(times) if times else 0

    # Log statistics
    logger.info(f"Mean Time: {mean_time:.6f} seconds")
    logger.info(f"Median Time: {median_time:.6f} seconds")
    logger.info(f"Standard Deviation: {stdev_time:.6f} seconds")

    # Print statistics
    print(f"Mean Time: {mean_time:.6f} seconds")
    print(f"Median Time: {median_time:.6f} seconds")
    print(f"Standard Deviation: {stdev_time:.6f} seconds")
    print(f"Count of matches: {match_count}")
    print(f"Count of Outside clusters: {len(points_stats) - match_count}")

# Register the save_stats function to be called on script termination
atexit.register(save_stats)


# Run benchmark
for point in tqdm(test_points, desc="Benchmarking"):
    test_point = Point(point['longitude'], point['latitude'])
    start_time = time.time()
    cluster_name = check_point_in_clusters(test_point)
    end_time = time.time()
    times.append(end_time - start_time)
    points_stats.append({"points": point, "cluster": cluster_name})

# Call save_stats explicitly to log final stats if script completes normally
save_stats()
