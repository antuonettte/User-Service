import json

def lambda_handler(event, context):
    # successful deployment
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
