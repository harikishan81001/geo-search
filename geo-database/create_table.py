from botocore.exceptions import ClientError
from db import dynamodb, cities_table_name

try:
    dynamodb.create_table(
        TableName=cities_table_name,
        KeySchema=[
            {"AttributeName": "ParentCell", "KeyType": "HASH"},
            {"AttributeName": "CellLocationIndex","KeyType": "RANGE"}
        ],   
        AttributeDefinitions=[
            {"AttributeName": "ParentCell", "AttributeType": "S"},
            {"AttributeName": "CellLocationIndex", "AttributeType": "S"},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 200,
            'WriteCapacityUnits': 200
        }
    )
except ClientError as e:
    print(e)


print("Tables created successfully")
