import json
import pymysql
import logging
from collections import defaultdict
import requests

#Scripts
# Insert the follow relationship into the follower table
insert_sql = '''
INSERT INTO followers (user_id, follower_id)
VALUES (%s, %s)
'''

# Increment following_count for follower
increment_follower_sql = '''
UPDATE users
SET following_count = following_count + 1
WHERE id = %s
'''

# Increment followers_count for followed
increment_followed_sql = '''
UPDATE users
SET follower_count = follower_count + 1
WHERE id = %s
'''

# Check if follow relationship exists
check_relationship_sql = '''
SELECT COUNT(*) FROM followers
WHERE user_id = %s AND follower_id = %s
'''

# Constants
POSTS_DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
POSTS_DB_USER = 'admin'
POSTS_DB_PASSWORD = 'FrostGaming1!'
POSTS_DB_NAME = 'post_db'

MEDIA_DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
MEDIA_DB_USER = 'admin'
MEDIA_DB_PASSWORD = 'FrostGaming1!'
MEDIA_DB_NAME = 'media_metadata_db'

COMMENT_DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
COMMENT_DB_USER = 'admin'
COMMENT_DB_PASSWORD = 'FrostGaming1!'
COMMENT_DB_NAME = 'comment_db'

USER_DB_HOST = 'car-network-db.c5kgayasi5x2.us-east-1.rds.amazonaws.com'
USER_DB_USER = 'admin'
USER_DB_PASSWORD = 'FrostGaming1!'
USER_DB_NAME = 'user_db'

DOMAIN_ENDPOINT = 'vpc-car-network-open-search-qkd46v7okrwchflkznxsldkx4y.aos.us-east-1.on.aws'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        http_method = event['httpMethod']
        path = event.get('resource')
        query_parameters = event['queryStringParameters']

        if http_method == 'GET':
            if path == '/user-management/user':
                logger.info('Getting User by ID')
                
                id = query_parameters.get('id')
                
                return get_user_by_id(id)
            elif path == '/user-management/users':
                logger.info("Getting all users")
                return get_all_users(event)
        
        elif http_method == 'POST':
            
            if path == '/user-management/user':
                user_data = json.loads(event['body'])
                return create_user(user_data)
            
            elif path == '/user-management/followers':
                follower_id = query_parameters.get('follower_id')
                following_id = query_parameters.get('following_id')
                return create_follow_relationship(following_id, follower_id)
            
        elif http_method == 'DELETE':
            path = event.get('resource')
            if path == '/user-management/user':
                user_id = query_parameters.get('user_id')
                return delete_user_data(user_id)
            else:
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'Unauthorized Resource Path'})
                }
        else:
            return {
                'statusCode': 405,
                'body': json.dumps({'error': 'Method Not Allowed'})
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    
def get_user_by_id(user_id):
    connection = pymysql.connect(
        host=USER_DB_HOST,
        user=USER_DB_USER,
        password=USER_DB_PASSWORD,
        database=USER_DB_NAME
    )
    
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, username, email, first_name, last_name, profile_picture_url, bio, location, date_of_birth, phone_number, status, follower_count, following_count FROM users WHERE id = %s"
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            
            if not result:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': 'User not found'})
                }
            
            user = {
                'id': result[0],
                'username': result[1],
                'email': result[2],
                'first_name': result[3],
                'last_name': result[4],
                'profile_picture': result[5],
                'bio': result[6],
                'location': result[7],
                'dob': result[8].isoformat() if result[8] else None,
                'phone_number': result[9],
                'status': result[10],
                'follower_count': result[11],
                'following_count': result[12]
            }
            
            return {
                'statusCode': 200,
                'body': json.dumps({'user': user})
            }
        
    except Exception as e:
        logger.error(f"Error fetching user with id {user_id}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to retrieve user'})
        }
    finally:
        connection.close()
    
def get_post_ids_for_user(user_id, domain_endpoint):
    url = f"https://{domain_endpoint}/posts/_search"
    headers = {"Content-Type": "application/json"}
    payload = {
        "_source": ["id"],
        "query": {
            "term": {
                "user_id": user_id
            }
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    
    if response.status_code == 200:
        search_results = response.json()
        post_ids = [hit["_source"]["id"] for hit in search_results["hits"]["hits"]]
        return tuple(post_ids)
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")
    
def delete_user_data(user_id):
    # Delete user from users table
    delete_user_sql = '''
    DELETE FROM users
    WHERE id = %s
    '''
    
    # Delete posts from posts table
    delete_posts_sql = '''
    DELETE FROM posts
    WHERE user_id = %s
    '''
    
    # Delete follower relationships
    delete_follower_sql = '''
    DELETE FROM followers
    WHERE user_id = %s OR follower_id = %s
    '''
    
    # Delete comments related to user's posts
    delete_comments_sql = '''
    DELETE FROM comments
    WHERE post_id IN %s
    '''

    # Decrement follower_count for users followed by the user being deleted
    decrement_followers_count_sql = '''
    UPDATE users 
    SET follower_count = follower_count - 1 
    WHERE id IN ( SELECT user_id FROM followers WHERE follower_id = %s )
    '''

    # Decrement following_count for users who followed the user being deleted
    decrement_following_count_sql = '''
    UPDATE users 
    SET following_count = following_count - 1 
    WHERE id IN ( SELECT follower_id FROM followers WHERE user_id = %s )
    '''
    
    user_conn = pymysql.connect(
        host=USER_DB_HOST,
        user=USER_DB_USER,
        password=USER_DB_PASSWORD,
        database=USER_DB_NAME
    )
    
    posts_conn = pymysql.connect(
        host=POSTS_DB_HOST,
        user=POSTS_DB_USER,
        password=POSTS_DB_PASSWORD,
        database=POSTS_DB_NAME
    )
    
    comments_conn = pymysql.connect(
        host=COMMENT_DB_HOST,
        user=COMMENT_DB_USER,
        password=COMMENT_DB_PASSWORD,
        database=COMMENT_DB_NAME
    )

    try:
        logger.info("Getting Post ID's")
        post_ids = get_post_ids_for_user(user_id, DOMAIN_ENDPOINT)
        logger.info(post_ids)
        
        with user_conn.cursor() as cursor:
            # Start transaction
            user_conn.begin()
            logger.info("Updating follower and following counts")
            cursor.execute(decrement_followers_count_sql, (user_id,))
            cursor.execute(decrement_following_count_sql, (user_id,))

            # Delete follower relationships
            logger.info("Deleting Follower Relationships")
            cursor.execute(delete_follower_sql, (user_id, user_id))

            # Delete user
            logger.info("Deleting User")
            cursor.execute(delete_user_sql, (user_id,))

            # Commit transaction
            user_conn.commit()
        
        # Delete comments
        if post_ids:
            logger.info("Deleting Comments")
            with comments_conn.cursor() as cursor:
                cursor.execute(delete_comments_sql, (post_ids,))
        
        # Delete posts
        logger.info("Deleting Posts")
        with posts_conn.cursor() as cursor:
            cursor.execute(delete_posts_sql, (user_id,))
        
        posts_conn.commit()
        comments_conn.commit()
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'User Successfully Deleted'})
        }
        
    except Exception as e:
        user_conn.rollback()
        posts_conn.rollback()
        comments_conn.rollback()
        logger.error(str(e))
        raise e
        
    finally:
        user_conn.close()
        posts_conn.close()
        comments_conn.close()
        
def get_all_users(event):
    connection = pymysql.connect(
        host=USER_DB_HOST,
        user=USER_DB_USER,
        password=USER_DB_PASSWORD,
        database=USER_DB_NAME
        )
                                     
    try:
        page = 1
        limit = 10
        
        query_parameters = event.get('queryStringParameters', {})
        if query_parameters:
            if 'page' in query_parameters:
                try:
                    page = int(query_parameters['page'])
                except ValueError:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': 'Page must be an integer'})
                    }
            if 'limit' in query_parameters:
                try:
                    limit = int(query_parameters['limit'])
                except ValueError:
                    return {
                        'statusCode': 400,
                        'body': json.dumps({'error': 'Limit must be an integer'})
                    }
                
        offset = (page - 1) * limit

        
        
        with connection.cursor() as cursor:
            sql = "SELECT * FROM users LIMIT %s OFFSET %s"
            cursor.execute(sql, (limit, offset))
            users = cursor.fetchall()
            
            user_list = []
            
            if users:
                for user in users:
                    user_dict = {
                        'id': user[0],
                        'username': user[1],
                        'email': user[2],
                        'createdAt': user[3].strftime('%Y-%m-%d %H:%M:%S'),
                        'updatedAt': user[4].strftime('%Y-%m-%d %H:%M:%S'),
                        'first_name': user[5],
                        'last_name': user[6],
                        'profile_picture': user[7],
                        'bio': user[8],
                        'location': user[9],
                        'dob': user[10],
                        'phone_number': user[11],
                        'status': user[12],
                        'follower_count': user[13],
                        'following_count': user[14]
                    }
                    
                    user_list.append(user_dict)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'users': user_list})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        connection.close()

def create_follow_relationship(user_id, follower_id):
    conn = pymysql.connect(
        host=USER_DB_HOST,
        user=USER_DB_USER,
        password=USER_DB_PASSWORD,
        database=USER_DB_NAME
    )
    cursor = conn.cursor()
    
    try:

        if check_follow_relationship(user_id, follower_id, cursor):
            return {
                'statusCode': 400,
                'body': json.dumps({'Message': 'Follow relationship already exists'})
            }
        
        logger.info("updating follow relationship")
        cursor.execute(insert_sql, (user_id, follower_id))
        logger.info("Incrementing Followed User Followers")
        cursor.execute(increment_followed_sql, (user_id))
        logger.info("Incrementing follower following count")
        cursor.execute(increment_follower_sql, (follower_id))

        conn.commit()
        
        return {
            'statusCode':201,
            'body':json.dumps({'Message':'Successfully created follower relationship'})
        }
    except Exception as e:
        logger.error(str(e))
        conn.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    
def check_follow_relationship(user_id, follower_id, cursor):
    cursor.execute(check_relationship_sql, (user_id, follower_id))
    result = cursor.fetchone()
    return result[0] > 0

def create_user(user_data):
    connection = pymysql.connect(
        host=USER_DB_HOST,
        user=USER_DB_USER,
        password=USER_DB_PASSWORD,
        database=USER_DB_NAME
        )
    try:
        logger.info("Creating user")
        logger.info(user_data)

        # Validate required fields
        logger.info("Validating Fields")
        required_fields = ['username', 'email', 'first_name']
        for field in required_fields:
            if field not in user_data:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': f'Missing required field: {field}'})
                }

        # Set default values for optional fields
        optional_fields = [
            'last_name', 'profile_picture', 'bio', 'location', 'dob',
            'phone_number', 'status', 'follower_count', 'following_count'
        ]
        for field in optional_fields:
            if field not in user_data:
                user_data[field] = None

        # Default values for follower_count and following_count
        if user_data['follower_count'] is None:
            user_data['follower_count'] = 0
        if user_data['following_count'] is None:
            user_data['following_count'] = 0

        # Set status to 'active' if not provided
        if user_data['status'] is None:
            user_data['status'] = 'active'

        

        
        logger.info("Inserting into db")
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO users (
                username, email, first_name, last_name,
                profile_picture_url, bio, location, date_of_birth, phone_number, status,
                follower_count, following_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                user_data['username'], user_data['email'],
                user_data['first_name'], user_data['last_name'], user_data['profile_picture'],
                user_data['bio'], user_data['location'], user_data['dob'], user_data['phone_number'],
                user_data['status'], user_data['follower_count'], user_data['following_count']
            ))
            connection.commit()
            
            # Retrieve the ID of the newly created user
            user_id = cursor.lastrowid

        return {
            'statusCode': 201,
            'body': json.dumps({'message': 'User created successfully', 'user_id': user_id})
        }

    except Exception as e:
        connection.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        connection.close()

# def delete_user_data(user_id):
#     # Delete user from users table
#     delete_user_sql = '''
#     DELETE FROM users
#     WHERE id = %s
#     '''
    
#     # Delete posts from posts table
#     delete_posts_sql = '''
#     DELETE FROM posts
#     WHERE user_id = %s
#     '''
    
#     # Delete follower relationships
#     delete_follower_sql = '''
#     DELETE FROM followers
#     WHERE user_id = %s OR follower_id = %s
#     '''
    
#     # Delete comments related to user's posts
#     delete_comments_sql = '''
#     DELETE FROM comments
#     WHERE post_id IN %s
#     '''
    
#     user_conn = pymysql.connect(
#         host=USER_DB_HOST,
#         user=USER_DB_USER,
#         password=USER_DB_PASSWORD,
#         database=USER_DB_NAME
#     )
    
#     posts_conn = pymysql.connect(
#         host=POSTS_DB_HOST,
#         user=POSTS_DB_USER,
#         password=POSTS_DB_PASSWORD,
#         database=POSTS_DB_NAME
#     )
    
#     comments_conn = pymysql.connect(
#         host=COMMENT_DB_HOST,
#         user=COMMENT_DB_USER,
#         password=COMMENT_DB_PASSWORD,
#         database=COMMENT_DB_NAME
#     )

#     try:
        
#         logger.info("Getting Post ID's")
#         post_ids = get_post_ids_for_user(user_id, DOMAIN_ENDPOINT)
#         logger.info(post_ids)
        
#         # Delete comments
#         logger.info("Deleting Comments")
#         with comments_conn.cursor() as cursor:
#             cursor.execute(delete_comments_sql, ( post_ids, ))
        
#         # Delete posts
#         logger.info("Deleting Posts")
#         with posts_conn.cursor() as cursor:
#             cursor.execute(delete_posts_sql, (user_id,))
        
#         # Delete follower relationships
#         logger.info("Deleting Follower Relationships")
#         with user_conn.cursor() as cursor:
#             cursor.execute(delete_follower_sql, (user_id, user_id))
        
#         # Delete user
#         logger.info("Deleting User")
#         with user_conn.cursor() as cursor:
#             cursor.execute(delete_user_sql, (user_id,))
        
        
        
#         user_conn.commit()
#         posts_conn.commit()
#         comments_conn.commit()
        
#         return {
#             'statusCode': 200,
#             'body': json.dumps({'message': 'User Successfully Deleted'})
#         }
        
#     except Exception as e:
#         user_conn.rollback()
#         posts_conn.rollback()
#         comments_conn.rollback()
#         logger.error(str(e))
#         raise e
#     finally:
#         user_conn.close()
#         posts_conn.close()
#         comments_conn.close()

# def create_user(user_data):
#     # Extract user data from request body
#     if ('username' not in user_data) or ('email' not in user_data) or ('first_name' not in user_data) or ('last_name' not in user_data):
#         return {
#             'statusCode': 400,
#             'body': json.dumps({'error': 'Please Enter Required Fields'}) 
#         }

#     username = user_data.get('username')
#     email = user_data.get('email')
#     first_name = user_data.get('first_name')
#     last_name = user_data.get('last_name')
#     profile_picture = user_data.get('profile_picture', None)
#     bio = user_data.get('bio', None)
#     location = user_data.get('location', None)
#     dob = user_data.get('dob', 'null')
#     phone_number = user_data.get('phone_number', None)
#     status = user_data.get('status', 'active')
#     follower_count = user_data.get('follower_count', 0)
#     following_count = user_data.get('following_count', 0)
    
#     # Connect to the database
#     conn = pymysql.connect(
#         host=USER_DB_HOST,
#         user=USER_DB_USER,
#         password=USER_DB_PASSWORD,
#         database=USER_DB_NAME
#     )
    
#     try:
#         logger.info("Inserting user into db")
#         with conn.cursor() as cursor:
#             # SQL statement to insert user into users table
#             sql = '''
#             INSERT INTO users (username, email, first_name, last_name, profile_picture_url, bio, location, date_of_birth, phone_number, status, follower_count, following_count)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             '''
#             # Execute the SQL statement
#             cursor.execute(sql, (username, email, first_name, last_name, profile_picture, bio, location, dob, phone_number, status, follower_count, following_count))
        
#         # Commit changes to the database
#         conn.commit()
        
#         # Return success response
#         return {
#             'statusCode': 201,
#             'body': json.dumps({'message': 'User created successfully'})
#         }
    
#     except Exception as e:
#         logger.error(f"Error creating user: {str(e)}")
#         # Rollback the transaction in case of error
#         conn.rollback()
        
#         # Return error response
#         return {
#             'statusCode': 500,
#             'body': json.dumps({'error': 'Failed to create user'})
#         }
    
#     finally:
#         # Close database connection
#         conn.close()
