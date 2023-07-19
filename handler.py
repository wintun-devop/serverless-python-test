import json
import urllib.parse
import boto3
import csv


s3 = boto3.client('s3')
ses = boto3.client('ses')


def main(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    #handling file type
    key_split=key.split(".")
    file_type=key_split[len(key_split) - 1]
    print ("BucketName>>",bucket," Key is ",key)
    if (file_type=="csv"):
        # csv operation
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read().decode('utf-8').splitlines()
            records = csv.reader(data) #4
            headers = next(records) #5
            print('headers: %s' % (headers))
            for eachRecord in records:
                print(eachRecord)
            mailer()
            return response['ContentType']
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e
    else:
        message = "We received {file_name} from {s3_bucket}.This is not csv file.".format(s3_bucket=bucket,file_name=key)
        print("file type error",message)
        return message

#ses main function 
def mailer():
    response = ses.send_email(
        Source='sender@gmail.com',
        Destination={
        'ToAddresses': ['receiver1@outlook.com'],
        # 'CcAddresses': ['receiver2@outlook.com'],
        },
        ReplyToAddresses=['receiver@outlook.com'],
        Message={
            'Subject': {
            'Data': 'Test Email',
            'Charset': 'utf-8'
            },
            'Body': {
            'Text': {
                'Data': 'hello world',
                'Charset': 'utf-8'
            },
            'Html': {
                'Data': 'hello world',
                'Charset': 'utf-8'
            }
            }
        }
    )
    print(response)
