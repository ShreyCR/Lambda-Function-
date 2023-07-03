import json
import mysql.connector
import os
import boto3
import csv

## This is the tool
def lambda_handler(event, context):
    print("event collected is {}".format(event))
    for record in event['Records'] :
        s3_bucket = record['s3']['bucket']['name']
        print("Bucket name is {}".format(s3_bucket))
        s3_key = record['s3']['object']['key']
        print("Bucket key name is {}".format(s3_key))
        from_path = "/tmp/{}".format(s3_key)
        print("from path {}".format(from_path))

        #initiate s3 client 
        s3 = boto3.client('s3')

        #Download object to the file    
        s3.download_file(s3_bucket, s3_key, from_path)
        print("donwloaded successfully....")
        
        dbname = os.getenv('dbname')
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')
        db_table = os.getenv('tablename')
        connection =  mysql.connector.connect(database = dbname,
                                       host = host,
                                       port = '3306',
                                       user = user,
                                       password = password)
                                       
        print('after connection....')
        curs = connection.cursor()
        print('after cursor....')


    with connection:
        with open(from_path, 'r') as file:
            csv_data = csv.DictReader(file)
            for row in csv_data:
                uuid = row['uuid']
                user_name = row['user_name']
                state_name = row['state_name']
                constituency_name = row['constituency_name']
                house = row['house']
                user_email = row['user_email']
                user_contact = row['user_contact']
                role = row['role']
                member_added = int(row['memberAdded'])
                followers = int(row['followers'])
                event_organized = int(row['eventOrganized'])
                op_eds = int(row['opEds'])
                books_published = int(row['bookspublished'])
                development_projects = int(row['developmentprojects'])
                media_coverage = int(row['mediaCoverage'])
                twitter_performance = int(row['twitterPerformance'])
                donation = int(row['donation'])
                donation_count = int(row['donationCount'])
                share = int(row['share'])
                event_interest = int(row['eventInterest'])
                initiative_reports = int(row['initiativeReports'])
                date = row['date']

                query = """
                    INSERT INTO {db_table} (
                        uuid, user_name, state_name, constituency_name, house, user_email,
                        role, member_added, followers, event_organized, op_eds,
                        books_published, development_projects, media_coverage, twitter_performance,
                        donation, donation_count, share, event_interest, initiative_reports, date
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """.format(db_table=db_table)

                curs.executemany(query, (
                    uuid, user_name, state_name, constituency_name, house, user_email,
                    role, member_added, followers, event_organized, op_eds,
                    books_published, development_projects, media_coverage, twitter_performance,
                    donation, donation_count, share, event_interest, initiative_reports, date
                ))

    connection.commit()
    connection.close()

    return {
        'statusCode': 200,
        'body': 'CSV data loaded into RDS successfully.'
    }
