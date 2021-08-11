const AWS = require('aws-sdk');

const dynamo = new AWS.DynamoDB.DocumentClient();

const TABLE_NAME = 'shopnob-main-data';
const MERCHANT_OFFER_PK = 'MERCHANT_OFFER#';
const OFFER_SK_PREFIX = 'OFFER#';

const MERCHANT_PK = 'MERCHANT';
const MERCHANT_SK_PREFIX = 'MERCHANT#';

const MERCHANT_LOCATION_PK = 'MERCHANT#';
const MERCHANT_LOCATION_SK_PREFIX = 'MERCHANT_LOCATION#';

const OFFER_COUPON_PK = 'OFFER_COUPON';
const COUPON_CODE_SK_PREFIX = 'COUPON#';

const USER_COUPON_PK = 'USER_COUPON#';
const CCODE_SK_PREFIX = 'COUPON_CODE#';

exports.handler = async(event) => {
    let body;
    let statusCode = '200';
    let errCode = '400';
    const headers = {
        'Content-Type': 'application/json',
    };

    try {
        switch (event.httpMethod) {
        case 'POST':
            const { code, phone  } = JSON.parse(event.body);
            if (code && phone) {
                const result = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: OFFER_COUPON_PK,
                        sortkey: `${COUPON_CODE_SK_PREFIX}${code}`,
                    },
                }).promise();
                

                if (result && result.Item) {
                    const offerCoupon = result.Item;

                    if (offerCoupon.isAssigned) {
                        errCode = '403';
                        throw new Error(`Coupon code : ${code} is already activated`);
                    } else {
                        // create new user coupon entry
                        // 1. get offer
                        const { offerId, merchantId } = offerCoupon;
                        const offerResult = await dynamo.get({
                            TableName: TABLE_NAME,
                            Key: {
                                partitionkey: `${MERCHANT_OFFER_PK}${merchantId}`,
                                sortkey: `${OFFER_SK_PREFIX}${offerId}`,
                            },
                        }).promise();
                        
                        body = offerResult.Item;

                        // set isAssigned to true in offer coupon
                        /*
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
                            merchantLocationId,
                            currentIndex: 0,
                        }
                    }).promise();

                    statusCode = '201';
                        */
                    }
                    
                    
                }
                else {
                    errCode = '404';
                    throw new Error(`Invalid coupon code`);
                }
            }
            else {
                errCode = '400';
                throw new Error(`Phone number and Coupon code missing in request body`);
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
