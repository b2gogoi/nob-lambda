const AWS = require('aws-sdk');

const dynamo = new AWS.DynamoDB.DocumentClient();

const TABLE_NAME = 'shopnob-main-data';
const MERCHANT_OFFER_PK = 'MERCHANT_OFFER#';
const OFFER_SK_PREFIX = 'OFFER#';

const MERCHANT_PK = 'MERCHANT';
const MERCHANT_SK_PREFIX = 'MERCHANT#';

const MERCHANT_LOCATION_PK = 'MERCHANT#';
const MERCHANT_LOCATION_SK_PREFIX = 'MERCHANT_LOCATION#';


const validateOffer = (offer) => {
    return true;
}

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
            if (merchantId) {
                const result = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: MERCHANT_PK,
                        sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                    },
                }).promise();
                

                if (result && result.Item) {
                    const merchant = result.Item;
                    const offer = JSON.parse(event.body);
                    const { offerId, isAllLocation} = offer;
                    let { merchantLocationId } = offer;

                    if (merchantId !== offer.merchantId) {
                        throw new Error(`Merchant Id in query params[${merchantId}] and request body[${offer.merchantId}] are not same`);
                    }

                    // If all location, then get the defaultLocation from merchant
                    if (isAllLocation) {
                        merchantLocationId = merchant.defaultLocationId;
                    }
                    else {
                    // merchantLocationId must be present
                        if (merchantLocationId) {
                            // verify if locationId is for that merchant
                            const merchantLocationResult = await dynamo.get({
                                TableName: TABLE_NAME,
                                Key: {
                                    partitionkey: `${MERCHANT_LOCATION_PK}${merchantId}`,
                                    sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${merchantLocationId}`,
                                },
                            }).promise();

                            if (!(merchantLocationResult && merchantLocationResult.Item)) {
                                throw new Error(`Merchant Location Id${merchantLocationId} does not belong to merchant${merchant.name}-${merchantId}`);
                            }
                        }
                        else {
                            throw new Error(`Merchant Location Id is required for non ALL locations offer`);
                        }

                    }

                    await dynamo.put({
                        TableName: TABLE_NAME,
                        Item: {
                            partitionkey: `${MERCHANT_OFFER_PK}${merchantId}`,
                            sortkey: `${OFFER_SK_PREFIX}${offerId}`,
                            ...offer,
                            merchantLocationId
                        }
                    }).promise();

                    statusCode = '201';
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
