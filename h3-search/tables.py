import boto3

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
            'AttributeName': 'cluster_id',
            'KeyType': 'HASH'  # Partition key
        },
        {
            'AttributeName': 'city_id',
            'KeyType': 'RANGE'  # Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'cluster_id',
            'AttributeType': 'S'
        },
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

print("Tables created")

