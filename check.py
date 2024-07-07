import boto3
from shapely.geometry import Point, shape

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource('dynamodb')

# Retrieve clusters within a city
def get_clusters_in_city(city_id):
    table = dynamodb.Table('Clusters')
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('city_id').eq(city_id)
    )
    return response['Items']

# Check if point is in any cluster polygon
def check_point_in_cluster(longitude, latitude, city_id):
    point = Point(longitude, latitude)
    clusters = get_clusters_in_city(city_id)
    
    for cluster in clusters:
        polygon = shape(json.loads(cluster['cluster_polygon']))
        if polygon.contains(point):
            return cluster['cluster_name']
    
    return None

# Example usage
longitude = -73.935242
latitude = 40.730610
city_id = '1'
cluster_name = check_point_in_cluster(longitude, latitude, city_id)
print("Point is in cluster:", cluster_name)

