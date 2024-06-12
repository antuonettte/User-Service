import json
import boto3
import pymysql
import requests

# Database configuration
db_host = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
db_user = 'admin'
db_password = 'FrostGaming1!'
db_name = 'user_db'

# Connect to the database
conn = pymysql.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name
)
cursor = conn.cursor()

def lambda_handler(event, context):
    # Determine the HTTP method
    http_method = event['httpMethod']
    
    if http_method == 'GET':
        # Handle GET request
        user_id = event['pathParameters']['id']
        return get_user(user_id)
    
    elif http_method == 'POST':
        # Handle POST request
        user_data = json.loads(event['body'])
        return create_user(user_data)
    
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Unsupported HTTP method')
        }

def get_user(user_id):
    try:
        # Retrieve user from the database
        cursor.execute(f"SELECT * FROM users WHERE id = {user_id}")
        user = cursor.fetchone()
        if user:
            return {
                'statusCode': 200,
                'body': json.dumps(user)
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps('User not found')
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def create_user(user_data):
    try:
        # Insert new user into the database
        cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s)", (user_data['name'], user_data['email']))
        conn.commit()
        return {
            'statusCode': 201,
            'body': json.dumps('User created successfully')
        }
    except Exception as e:
        conn.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
