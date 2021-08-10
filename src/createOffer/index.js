const AWS = require('aws-sdk');

const dynamo = new AWS.DynamoDB.DocumentClient();

const TABLE_NAME = 'shopnob-main-data';
const MERCHANT_OFFER_PK = 'MERCHANT_OFFER#';
const OFFER_SK_PREFIX = 'OFFER#';

exports.handler = async(event) => {
    let body;
    let statusCode = '200';
    let errCode = '400';
    const headers = {
        'Content-Type': 'application/json',
    };

    let merchantId;

    if (event.queryStringParameters) {
        merchantId = event.queryStringParameters.merchantId;
    }

    try {
        switch (event.httpMethod) {
        
        case 'POST':
            const offer = JSON.parse(event.body);

            await dynamo.put({
                TableName: TABLE_NAME,
                Item: {
                    partitionkey: `${MERCHANT_OFFER_PK}${merchantId}`,
                    sortkey: `${OFFER_SK_PREFIX}${offer.offerId}`,
                    ...offer
                }
            }).promise();

            statusCode = '201';
            break;
        
        default:
            throw new Error(`Unsupported method "${event.httpMethod}"`);
        }
    }
    catch (err) {
        statusCode = errCode;
        body = err.message;
    }
    finally {
        body = JSON.stringify(body);
    }

    return {
        statusCode,
        body,
        headers,
    };
};
