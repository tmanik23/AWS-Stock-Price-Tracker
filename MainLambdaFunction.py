import json
import yfinance as yf
import boto3
from boto3.dynamodb.conditions import Key
import dateutil.tz
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):


    client = boto3.resource('dynamodb') # initiate DynamoDB via boto3
    table = client.Table('StockPrices') # StockPrices DynamoDB table
    symbol = 'LCID' # defines stock ticker to get info from

    
    # query for latest record in StockPrices
    output = table.query(KeyConditionExpression = Key('Symbol').eq(symbol), ScanIndexForward = False, Limit = 1) 
    
    # get the Price of the most recent record in StockPrices
    items = output['Items']
    for item in items:
        price1 = item['Price']
    
  
    stock = yf.Ticker(symbol)   # input the stock ticker we want the yfinance library to get info about
    price  =  stock.info['regularMarketPrice']  # get most recent stock price from Yahoo Finance
    
    # converts "price" variable from float to decimal
    # DynamoDB does not take in float values
    price2 = json.loads(json.dumps(price), parse_float=Decimal)
    
    # get the current EST date and time in yy–mm-dd hours:minutes:seconds format
    timeZone = dateutil.tz.gettz('US/Eastern')
    easternTime = datetime.now(tz=timeZone)
    timeStamp = easternTime.strftime('%Y-%m-%d %H:%M:%S')
    
    
    cadence = '5m'  # define how often Event Bridge triggers this Lambda function
    cadenceStamp = cadence + "_" + timeStamp # concatenate data of current time with cadence
    
    input = {'Symbol': symbol,'TimeStamp': timeStamp, 'CadenceStamp': cadenceStamp,'Price': round(price2, 2)} # define values to be PUT in StockPrices table
    response = table.put_item(Item=input)   # PUTs time, symbol, and price data to StockPrices table 
    
    # get ratio between latest stock price against previous stock price
    percent_diff = (float(price2) - float(price1))
    percent = percent_diff / float(price1)
    percent_decimal = json.loads(json.dumps(percent*100), parse_float=Decimal)
    percent_round = round(percent_decimal, 2)
    
    # input the delta (percentage change) between the stock prices to the Change table
    table_percent = client.Table('Change')
    response = table_percent.update_item(
        Key={
            'Symbol': symbol,
            'Cadence': cadence
        },
        AttributeUpdates={
            'Percent': {"Action": "PUT", "Value": percent_round}
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }