import boto3

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-west-2', endpoint_url='http://127.0.0.1:4566')


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

# Wait until the tables exist
cities_table.meta.client.get_waiter('table_exists').wait(TableName='Cities')
clusters_table.meta.client.get_waiter('table_exists').wait(TableName='Clusters')

