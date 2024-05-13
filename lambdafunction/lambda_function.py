import json
import pymysql

def lambda_handler(event, context):
    
    # Connect to the database
    write_connection = pymysql.connect(
        host='user-service-db.cluster-c5kgayasi5x2.us-east-1.rds.amazonaws.com',
        user='admin',
        password='OHX$vJ8$9yv3',
        database='user-service-db'
    )
    read_connection = pymysql.connect(
        host='user-service-db.cluster-ro-c5kgayasi5x2.us-east-1.rds.amazonaws.com',
        user='admin',
        password='OHX$vJ8$9yv3', 
        database='user-service-db'
    )
    
    write_cursor = write_connection.cursor()
    
    # with open('users_table.sql', 'r') as file:
    #     script = file.read()
    #     cursor.execute(script)
        
    # connection.commit()
    
    # cursor.close()
    # connection.close()

    # # Create a cursor object
    # cursor = connection.cursor()

    # # Execute a query
    # cursor.execute('SELECT * FROM your_table')

    # # Fetch the results
    # results = cursor.fetchall()

    # # Close the cursor and connection
    # cursor.close()
    # connection.close()
    
    return {
        'statusCode': 200,
        'body': json.dumps('db test')
    }
