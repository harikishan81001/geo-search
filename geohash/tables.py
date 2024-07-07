import boto3

# Initialize a session using LocalStack's DynamoDB with dummy credentials
from db import dynamodb


# Create Cities Table
cities_table = dynamodb.create_table(
    TableName='Cities',
    KeySchema=[
        {
            'AttributeName': 'city_id',
            'KeyType': 'HASH'  # Partition key
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

# Create Clusters Table
clusters_table = dynamodb.create_table(
    TableName='Clusters',
    KeySchema=[
        {
            'AttributeName': 'geohash',
            'KeyType': 'HASH'  # Partition key
        },
        {
            'AttributeName': 'cluster_id',
            'KeyType': 'RANGE'  # Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'geohash',
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

print("Tables created")
