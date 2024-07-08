import csv
from shapely.geometry import Polygon
import h3
from db import cities_table, clusters_table
from decimal import Decimal


def convert_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, list):
        return [convert_to_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_to_decimal(v) for k, v in obj.items()}
    return obj


def polygon_to_h3_indexes(polygon, resolution):
    """
    Convert a shapely polygon to a list of H3 indexes at the given resolution.
    """
    polygon_geojson = {
        'type': 'Polygon',
        'coordinates': [list(polygon.exterior.coords)]
    }
    return list(h3.polyfill(polygon_geojson, resolution))


with open('data/ncr.csv', 'r') as cities_file:
    cities_reader = csv.DictReader(cities_file)
    for row in cities_reader:
        city_id = row['city_id']
        city_name = row['city_name']
        city_center_lat = float(row['city_center_lat'])
        city_center_lng = float(row['city_center_lng'])
        city_bounds = eval(row['city_bounds'])
        
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

with open('data/ncr-clusters.csv', 'r') as clusters_file:
    clusters_reader = csv.DictReader(clusters_file)
    for row in clusters_reader:
        cluster_id = row['cluster_id']
        city_id = row['city_id']
        cluster_name = row['cluster_name']
        cluster_polygon = Polygon(eval(row['cluster_polygon']))
        
        h3_indexes = polygon_to_h3_indexes(cluster_polygon, resolution=9)
        
        for h3_index in h3_indexes:
            clusters_table.put_item(
                Item=convert_to_decimal({
                    'h3_index': h3_index,
                    'cluster_id': cluster_id,
                    'city_id': city_id,
                    'cluster_name': cluster_name,
                    'cluster_polygon': {
                        'type': 'Polygon',
                        'coordinates': eval(row['cluster_polygon'])
                    }
                })
            )

print("Data inserted successfully")
