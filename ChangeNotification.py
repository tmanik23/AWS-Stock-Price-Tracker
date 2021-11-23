import json
import boto3

def lambda_handler(event, context):

    # set up try/except handling
    try:
        # Iterate over each record
        for record in event['Records']:
            handle_modify(record)
    except Exception as e:
        print(e)
        
        return "Oops!"
        
# define function that detects updates in the Change DynamoDB table
def handle_modify(record):

    # extract data from the Change table DynamoDB stream
    newImage = record['dynamodb']['NewImage']
    Symbol  = newImage['Symbol']['S']
    newPercent = newImage['Percent']['N']   # get the latest percentage change
    PercentFloat = float(newPercent)    # convert newPercent from string to float
    print(PercentFloat)
    
    # define the percentage change thresold that user wants to get notified about
    desiredPercent = 1.5
    
    if (PercentFloat > desiredPercent):
        client = boto3.client('sns')
        response = client.publish(
            TargetArn = "arn:aws:sns:us-east-1:135181374938:StockNotifTest",
            Subject = "Your stock " + Symbol + " is " + str(PercentFloat) + "% up!",
            Message = "Take a look at the " + Symbol + " stock. That stock is HOT! It's " + str(PercentFloat) + "% up!"
            )
        
    else:
        print("Don't worry about the stock")