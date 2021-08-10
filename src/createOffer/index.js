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

/*
    @param [count] number coupon count, max 9975
    @param [padChars] string 3 char string for padding
    @returns Array
 */
const padSequence = (count, padChars, lastIndex) => {
    const paddedSequence = [];
    
    for (let i = 0; i < count; i++) {
        const seq = i + lastIndex + 24;
        if (seq < 10) {
            paddedSequence.push(`${padChars}${seq}`);
        } else if (seq < 100) {
            paddedSequence.push(`${padChars.substr(0,2)}${seq}`);
        } else if (seq < 1000) {
            paddedSequence.push(`${padChars.substr(0,1)}${seq}`);
        } else {
            paddedSequence.push(`${seq}`);
        }
    }
    return paddedSequence;
}

const genCouponCodes = (count, merchant, location, offer) => {
    const PAD_CHARS = `${offer.offerId.substr(0, 2)}${offer.offerId.substr(offer.offerId.length - 1)}`; // first 2 and last char od 
    const seq = padSequence(count, PAD_CHARS.toUpperCase(), offer.currentIndex);

	const first5 = merchant.category;

    // first 2 letters and last letter
    const fullMerchantName = merchant.name.split(' ').join('');
    const second3 = `${fullMerchantName.substr(0,2)}${fullMerchantName.substr(fullMerchantName.length - 1)}`.toUpperCase();
    const third1 = offer.type.substr(0,1);
    const fifth3 = `${location.branch.substr(0,2)}${location.branch.substr(location.branch.length - 1)}`.toUpperCase();

    return seq.map((code, i) => {
        const now = (new Date()).getTime();
        const addr = now % 10;
        const last3 = (now + (addr * i) + i) % 1000;
        const last3Str = `${last3}`;
        return `${second3}${first5}-${third1}${code}-${fifth3}${last3Str.padStart(3, '0')}`;
    })
}

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

    let merchantId = null;

    if (event.queryStringParameters) {
        merchantId = event.queryStringParameters.merchantId;
    }

    try {
        switch (event.httpMethod) {
        case 'GET':
            if (merchantId) {
                let queryParams = {
                    TableName: TABLE_NAME,
                    KeyConditionExpression: '#pk = :pk AND begins_with(sortkey, :sk)',
                    ExpressionAttributeNames: {
                        '#pk': 'partitionkey',
                    },
                    ExpressionAttributeValues: {
                        ':pk': `${MERCHANT_OFFER_PK}${merchantId}`,
                        ':sk': OFFER_SK_PREFIX,
                    },
                };
                const offersResult = await dynamo.query(queryParams).promise();
                    
                if (offersResult && offersResult.Items && offersResult.Items.length > 0) {
                    body = offersResult.Items.map(offer => {
                        let clone = { ...offer };
                        delete clone.partitionkey;
                        delete clone.sortkey;
                        return clone;
                    });
                } 
                else {
                    const result = await dynamo.get({
                        TableName: TABLE_NAME,
                        Key: {
                            partitionkey: MERCHANT_PK,
                            sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                        },
                    }).promise();
                

                    if (result && result.Item) {
                        console.log(`Merchant: ${result.Item.name}[${merchantId}] doesn't have any offers yet`);
                        body = [];
                    }
                    else {
                        errCode = '404';
                        throw new Error(`Not found : merchantId ${merchantId}`);
                    }
                }
            }
            else {
                errCode = '400';
                throw new Error(`MerchantId missing in queryParams`);
            }
            break;

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
                            merchantLocationId,
                            currentIndex: 0,
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

        case 'PUT':
            if (!event.queryStringParameters) {
                errCode = '400';
                throw new Error(`OfferId & MerchantId missing in queryParams`);
            }
            const { offerId } = event.queryStringParameters;
            if (offerId && merchantId) {
                const merchantResult = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: MERCHANT_PK,
                        sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                    },
                }).promise();
                
                if (!(merchantResult && merchantResult.Item)) {
                    errCode = '404';
                    throw new Error(`Not found : merchantId ${merchantId}`);
                }
                
                const merchant = merchantResult.Item;
                
                const offerResult = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: `${MERCHANT_OFFER_PK}${merchantId}`,
                        sortkey: `${OFFER_SK_PREFIX}${offerId}`,
                    },
                }).promise();
                
                if (!(offerResult && offerResult.Item)) {
                    errCode = '404';
                    throw new Error(`No such offer exists for OfferId : ${offerId}`);
                }
                
                const offer = offerResult.Item;
                const { merchantLocationId } = offer;

                const locationResult = await dynamo.get({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: `${MERCHANT_LOCATION_PK}${merchantId}`,
                        sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${merchantLocationId}`,
                    },
                }).promise();
                
                const location = locationResult.Item;
                
                const { count } = JSON.parse(event.body);
                
                const codes = genCouponCodes(count, merchant, location, offer);
                
                const requestItems = codes.map(code => ({
                    PutRequest: {
                        Item: {
                            partitionkey: OFFER_COUPON_PK,
                            sortkey: `${COUPON_CODE_SK_PREFIX}${code}`,
                            code,
                            offerId
                        },
                    },
                }));
                
                const result = await dynamo.batchWrite({
                    RequestItems: {
                        'shopnob-main-data': requestItems,
                    },
                }).promise();

                const total = offer.currentIndex + codes.length;

                const update = await dynamo.update({
                    TableName: TABLE_NAME,
                    Key: {
                        partitionkey: `${MERCHANT_OFFER_PK}${merchantId}`,
                        sortkey: `${OFFER_SK_PREFIX}${offerId}`,
                    },
                    UpdateExpression: 'SET currentIndex = :total',
                    ExpressionAttributeValues: {
                        ':total': total,
                    },
                    ReturnValues: 'ALL_NEW',
                }).promise();
                statusCode = '200';
                
                body = {
                    result,
                    success: `${codes.length} coupons generated and index update on offer`,
                    newOfferIndex: update.Attributes.currentIndex,
                };
                statusCode = '201';
            }
            else {
                errCode = '400';
                throw new Error(`OfferId and MerchantId, both are required in queryParams`);
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
