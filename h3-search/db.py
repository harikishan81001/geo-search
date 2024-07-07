import boto3

dynamodb = boto3.resource(
    'dynamodb',
    region_name='ap-south-1',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='dummy_access_key',
    aws_secret_access_key='dummy_secret_key'
)


cities_table_name = 'H3Cities'
clusters_table_name = 'H3Clusters'

cities_table = dynamodb.Table(cities_table_name)
clusters_table = dynamodb.Table(clusters_table_name)