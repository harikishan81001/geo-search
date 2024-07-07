import boto3
import folium
from shapely.geometry import shape, Polygon
from db import dynamodb


cities_table = dynamodb.Table('Cities')
clusters_table = dynamodb.Table('Clusters')

# Fetch all cities from DynamoDB
cities = []
response = cities_table.scan()
for item in response['Items']:
    city_id = item['city_id']
    city_name = item['city_name']
    city_center_lat = float(item['city_center']['latitude'])
    city_center_lng = float(item['city_center']['longitude'])
    city_bounds = item['city_bounds']['coordinates']
    
    cities.append({
        'city_id': city_id,
        'city_name': city_name,
        'city_center': (city_center_lat, city_center_lng),
        'city_bounds': Polygon(city_bounds)
    })

# Fetch all clusters from DynamoDB
clusters = []
response = clusters_table.scan()
for item in response['Items']:
    cluster_id = item['cluster_id']
    city_id = item['city_id']
    cluster_name = item['cluster_name']
    cluster_polygon = item['cluster_polygon']['coordinates']
    
    clusters.append({
        'cluster_id': cluster_id,
        'city_id': city_id,
        'cluster_name': cluster_name,
        'cluster_polygon': Polygon(cluster_polygon)
    })

# Create a map centered on the first city's center
map_center = [cities[0]['city_center'][0], cities[0]['city_center'][1]]
map = folium.Map(location=map_center, zoom_start=12)

# Plot cities
for city in cities:
    city_bounds_coords = [(lat, lng) for lng, lat in city['city_bounds'].exterior.coords]
    folium.Polygon(city_bounds_coords, color='blue', weight=2, opacity=0.5, fill=True, fill_opacity=0.2).add_to(map)
    folium.Marker(city['city_center'], popup=f"{city['city_name']} Center").add_to(map)

# Plot clusters
for cluster in clusters:
    cluster_polygon_coords = [(lat, lng) for lng, lat in cluster['cluster_polygon'].exterior.coords]
    folium.Polygon(cluster_polygon_coords, color='red', weight=2, opacity=0.5, fill=True, fill_opacity=0.2).add_to(map)
    folium.Marker(cluster['cluster_polygon'].centroid.coords[0][::-1], popup=f"{cluster['cluster_name']}").add_to(map)

# Save map to HTML file
map.save('map.html')
print("Map saved as map.html")
