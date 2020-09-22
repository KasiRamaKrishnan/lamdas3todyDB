import boto3

#creating a s3 object for s3 service
s3_service = boto3.client("s3")
#creating a dynamodb object for dynamodb service
dynamoDB = boto3.resource("dynamodb")
#creating a table object to get the table name from dynamodb
table = dynamoDB.Table("kasi")
client = boto3.client("dynamodb")

def lambda_handler(event,context):
    response = client.list_tables()
    if 'kasi' not in response['TableNames']:
        #print(__file__)
        table = dynamoDB.create_table(
            TableName='kasi',
            KeySchema=[
                {
                    'AttributeName': 'a',
                    'KeyType': 'HASH'  
                },
                {
                    'AttributeName': 'b',
                    'KeyType': 'RANGE'  
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'a',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'b',
                    'AttributeType': 'S'
                },
       
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 3,
                'WriteCapacityUnits': 3
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName='kasi')
    #get the bucketname & filename once the event is triggered
    bucketName = event["Records"][0]["s3"]["bucket"]["name"]
    fileName = event["Records"][0]["s3"]["object"]["key"]
    #getting the response from the file using get_object method
    res = s3_service.get_object(Bucket=bucketName,Key=fileName)
    #getting the file content from Body key of the response and decode it using decode method
    data = res["Body"].read().decode("utf-8")
    #splitting the file contents using "\n"
    lineSplit = data.split("\n")
    #iterate the splitted lines which is stored in list
    for line in lineSplit:
        #splitting each elements of the that list using ","
        element = line.split(",")
        #using put_item method, put the values to the corresponding table fields in dynamodb
        table.put_item(
           Item = {
                "a": element[0],
                "b": element[1],
                "c": element[2]
            }
        )
