import json

def lambda_handler(event, context):
    # successful deployment done
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
