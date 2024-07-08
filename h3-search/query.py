from shapely.geometry import Point, Polygon
import h3
import time
from tqdm import tqdm
from loguru import logger
import random
import statistics
import atexit
from db import clusters_table
import boto3


def get_clusters_by_h3_index(h3_index):
    response = clusters_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('h3_index').eq(h3_index)
    )
    return response['Items']

def check_point_in_clusters(point, resolution=9):
    """
    Check if a point lies within any cluster using H3.
    """
    h3_index = h3.geo_to_h3(point.y, point.x, resolution)
    import pdb; pdb.set_trace()
    clusters = get_clusters_by_h3_index(h3_index)
    print(clusters)
    for cluster in clusters:
        polygon_coordinates = eval(cluster['cluster_polygon']['coordinates'])
        polygon = Polygon(polygon_coordinates[0])
        if polygon.contains(point):
            return cluster['cluster_name']
    
    return None

# Test points
test_points = [
    {'latitude': 28.600792101087805, 'longitude': 77.04312829132186, 'description': 'Inside Dwarka'},
    {'latitude': 28.55103964613204, 'longitude': 76.97192393485614, 'description': 'Outside Delhi NCR'}
]

times = []
logger.add("benchmark.log", rotation="1 MB", backtrace=True, diagnose=True)

def save_stats():
    mean_time = statistics.mean(times) if times else 0
    median_time = statistics.median(times) if times else 0
    stdev_time = statistics.stdev(times) if times else 0

    logger.info(f"Mean Time: {mean_time:.6f} seconds")
    logger.info(f"Median Time: {median_time:.6f} seconds")
    logger.info(f"Standard Deviation: {stdev_time:.6f} seconds")

    print(f"Mean Time: {mean_time:.6f} seconds")
    print(f"Median Time: {median_time:.6f} seconds")
    print(f"Standard Deviation: {stdev_time:.6f} seconds")

atexit.register(save_stats)

for point in tqdm(test_points, desc="Benchmarking"):
    test_point = Point(point['longitude'], point['latitude'])
    start_time = time.time()
    cluster_name = check_point_in_clusters(test_point)
    end_time = time.time()
    times.append(end_time - start_time)

    if len(times) % 1000 == 0:
        logger.info(f"Processed {len(times)} points")

save_stats()
