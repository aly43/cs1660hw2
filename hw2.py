import boto3
import csv

s3 = boto3.resource('s3',
	aws_access_key_id='AKIA34IHFKM6HQGH4M25',
	aws_secret_access_key='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'
	)

try:
        s3.create_bucket(Bucket='cs1660hw2aly431', CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'
        })
except:
        pass

bucket = s3.Bucket('cs1660hw2aly431')

bucket.Acl().put(ACL='public-read')

##body = open(r"C:\Users\ajyan\OneDrive\CS1660\HW2\MasterCSV", 'rb')
##
##o = s3.Object('cs1660hw2aly431', 'MasterCSV').put(Body=body )
##
##s3.Object('cs1660hw2aly431', 'MasterCSV').Acl().put(Acl = 'public-read')
##
##dynb = boto3.resource('dynamodb',
##                      region_name='us-west-2',
##                      aws_access_key = 'AKIA34IHFKM6HQGH4M25',
##                      aws_secret_access_key = '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'
##                      )

dyndb = boto3.resource('dynamodb',
                       	aws_access_key_id='AKIA34IHFKM6HQGH4M25',
                        aws_secret_access_key='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',
                        region_name='us-west-2')


table = dyndb.create_table(
        TableName = 'DataTable',
        KeySchema = [
                { 'AttributeName': 'PartitionKey', 'KeyType': 'HASH' },
                { 'AttributeName': 'RowKey', 'KeyType': 'RANGE' }
                ],
        AttributeDefinitions=[
                { 'AttributeName': 'PartitionKey', 'AttributeType': 'S' },
                { 'AttributeName': 'RowKey', 'AttributeType': 'S' }
                ],
        ProvisionedThroughput={'ReadCapacityUnits': 5,'WriteCapacityUnits': 5}
        )

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')



table = dyndb.Table("DataTable")

urlbase = "https://cs1660hw2aly431.s3-us-west-2.amazonaws.com/"
with open(r"C:\Users\ajyan\OneDrive\CS1660\HW2\MasterCSV.csv", 'r') as csvfile:
        csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(csvf)
        for item in csvf:
                body = open(r"C:\Users\ajyan\OneDrive\CS1660\HW2\\"+item[4], 'rb')
                s3.Object('cs1660hw2aly431', item[4]).put(Body=body)
                md= s3.Object('cs1660hw2aly431', item[4]).Acl().put(ACL='public-read')
                url=urlbase+item[4]
                #print(item[0])
                
                metadata_item={'PartitionKey': item[0], 'RowKey': item[1],
                               'description': item[3], 'date' : item[2], 'url':url}
                print(metadata_item)
                table.put_item(Item=metadata_item)











