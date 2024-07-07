from db import dynamodb, cities_table_name, clusters_table_name


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

# Create Clusters Table
dynamodb.create_table(
    TableName=clusters_table_name,
    KeySchema=[
        {
            'AttributeName': 'h3_index',
            'KeyType': 'HASH'
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

print("Tables created successfully")
