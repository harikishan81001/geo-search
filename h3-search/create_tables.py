from botocore.exceptions import ClientError

from db import dynamodb, cities_table_name, clusters_table_name

try:
    dynamodb.create_table(
        TableName=cities_table_name,
        KeySchema=[
            {
                'AttributeName': 'city_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'city_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except ClientError as e:
    print(e)

try:
    # Create Clusters Table
    dynamodb.create_table(
        TableName=clusters_table_name,
        KeySchema=[
            {
                'AttributeName': 'h3_index',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'cluster_id',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'h3_index',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'cluster_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except ClientError as e:
    print(e)

print("Tables created successfully")
