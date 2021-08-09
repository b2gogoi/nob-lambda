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
        case 'GET':
            if (merchantId) {
                const result = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: MERCHANT_PK,
                        sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                    },
                }).promise();

                if (result && result.Item) {
                    let queryParams = {
                        TableName: TABLE_NAME,
                        KeyConditionExpression: '#pk = :pk AND begins_with(sortkey, :sk)',
                        ExpressionAttributeNames: {
                            '#pk': 'partitionkey',
                        },
                        ExpressionAttributeValues: {
                            ':pk': `${MERCHANT_LOCATION_PK}${merchantId}`,
                            ':sk': MERCHANT_LOCATION_SK_PREFIX,
                        },
                    };

                    const locations = await dynamo.query(queryParams).promise();
                    const { id, name, category, defaultLocationId, logoUrl } = result.Item;
                    body = {
                        id,
                        name,
                        category,
                        logoUrl,
                        defaultLocationId,
                        locations: locations.Items.map(loc => {
                            let clone = { ...loc };
                            delete clone.partitionkey;
                            delete clone.sortkey;
                            return clone;
                        })
                    };
                }
                else {
                    errCode = '404';
                    throw new Error(`Not found : merchantId ${merchantId}`);
                }
            }
            else {
                errCode = '400';
                throw new Error(`MerchantId missing in queryParams`);
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

            statusCode = '201';
            break;
        case 'PUT':
            if (merchantId) {
                const result = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: MERCHANT_PK,
                        sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                    },
                }).promise();

                if (result && result.Item) {
                    const req = JSON.parse(event.body);

                    // Add new location
                    if (req.locationId) {
                        const location = req;
                        await dynamo.put({
                            TableName: TABLE_NAME,
                            Item: {
                                partitionkey: `${MERCHANT_LOCATION_PK}${merchantId}`,
                                sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${location.locationId}`,
                                ...location
                            }
                        }).promise();
                        statusCode = '201';
                        body = { success: `A new branch: ${location.branch}, is added for merchant ${result.Item.name}` }
                    }

                    // Update Merchant logo
                    if (req.logoUrl) {
                        const update = await dynamo.update({
                            TableName: TABLE_NAME,
                            Key: {
                                partitionkey: MERCHANT_PK,
                                sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                            },
                            UpdateExpression: 'SET logoUrl = :logo',
                            ExpressionAttributeValues: {
                                ':logo': req.logoUrl,
                            },
                            ReturnValues: 'ALL_NEW',
                        }).promise();
                        statusCode = '200';
                        body = {
                            success: `Logo URL added/updated for merchant ${result.Item.name}`,
                            logoUrl: update.Attributes.logoUrl,
                        };
                    }
                }
                else {
                    errCode = '404';
                    throw new Error(`Not found : merchantId ${merchantId}`);
                }
            }
            else {
                errCode = '400';
                throw new Error(`MerchantId missing in queryParams`);
            }
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
