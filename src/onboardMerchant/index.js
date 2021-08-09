const AWS = require('aws-sdk');

const dynamo = new AWS.DynamoDB.DocumentClient();

const TABLE_NAME = 'shopnob-main-data';
const MERCHANT_PK = 'MERCHANT';
const MERCHANT_SK_PREFIX = 'MERCHANT#';

const MERCHANT_LOCATION_PK = 'MERCHANT#';
const MERCHANT_LOCATION_SK_PREFIX = 'MERCHANT_LOCATION#';

/**
 * Demonstrates a simple HTTP endpoint using API Gateway. You have full
 * access to the request and response payload, including headers and
 * status code.
 *
 * To scan a DynamoDB table, make a GET request with the TableName as a
 * query string parameter. To put, update, or delete an item, make a POST,
 * PUT, or DELETE request respectively, passing in the payload to the
 * DynamoDB API as a JSON body.
 */
exports.handler = async(event, context) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

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
        case 'PUT':
            if (merchantId) {
                const location = JSON.parse(event.body);
                body = await dynamo.put({
                    TableName: TABLE_NAME,
                    Item: {
                        partitionkey: MERCHANT_LOCATION_PK,
                        sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${merchantId}`,
                        ...location
                    }
                }).promise();
            }
            else {
                throw new Error(`MerchantId missing in queryParams`);
            }

            break;
        case 'GET':

            // if (merchantId) {
            //     await dynamo.query()
            // }
            // body = await dynamo.scan({ TableName: event.queryStringParameters.TableName }).promise();
            let queryParams = {
                TableName: TABLE_NAME,
                KeyConditionExpression: '#pk = :pk AND #sk = :sk',
                ExpressionAttributeNames: {
                    '#pk': 'partitionkey',
                    '#sk': 'sortkey',
                },
                ExpressionAttributeValues: {
                    ':pk': MERCHANT_PK,
                    ':sk': `${MERCHANT_SK_PREFIX}${merchantId}`,
                },
            };


            const result = await dynamo.query(queryParams).promise();
            if (result.Items && result.Items.length > 0) {
                body = result.Items[0];
            }
            else {
                errCode = '404';
                throw new Error(`Not found : merchantId ${merchantId}`);
            }
            break;
        case 'POST':
            const { id, name, category, locations } = JSON.parse(event.body);

            const defLocation = locations[0];

            body = await dynamo.batchWrite({
                RequestItems: {
                    'shopnob-main-data': [{
                            PutRequest: {
                                Item: {
                                    partitionkey: MERCHANT_PK,
                                    sortkey: `${MERCHANT_SK_PREFIX}${id}`,
                                    id,
                                    name,
                                    category,
                                    defaultLocationId: defLocation.locationId
                                },
                            },
                        },
                        {
                            PutRequest: {
                                Item: {
                                    partitionkey: `${MERCHANT_LOCATION_PK}${id}`,
                                    sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${defLocation.locationId}`,
                                    ...defLocation
                                },
                            },
                        },
                    ],
                },
            }).promise();

            /* const merchant = await dynamo.put({
                TableName: TABLE_NAME,
                Item: {
                    partitionkey: MERCHANT_PK,
                    sortkey: `${MERCHANT_SK_PREFIX}${id}`,
                    id,
                    name,
                    category,
                    defaultLocationId: defLocation.locationId
                }
            }).promise();

            const location = await dynamo.put({
                TableName: TABLE_NAME,
                Item: {
                    partitionkey: MERCHANT_LOCATION_PK,
                    sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${id}`,
                    ...defLocation
                }
            }).promise(); */

            // body = { ...merchant, location };
            statusCode = '201';
            break;
        case 'PUT':
            body = await dynamo.update(JSON.parse(event.body)).promise();
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
