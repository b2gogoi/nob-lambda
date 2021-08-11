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

function formatDate(date) {
    const year = date.getFullYear();
    const month = `${date.getMonth() + 1}`;
    const day = `${date.getDate()}`;
	return`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

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
                        errCode = '405';
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
                        
                        const offer = offerResult.Item;
                        const { activationStartDate, activationEndDate, 
                            desc,
                            startDate, endDate,
                            offerExpiryType, nDays,
                            orderUnit, minOrder, maxDiscount,
                            isAllLocation,
                            merchantLocationId
                         } = offer;
                        const activationRange = [(new Date(activationStartDate)).getTime(), (new Date(activationEndDate)).getTime()];

                        const now = new Date();
                        const today = formatDate(now);
                        const todayDate = new Date(today)
                        let tommorrowDate = new Date();
                        tommorrowDate.setTime(todayDate.getTime() + 86400000);
                        const tommorrow = formatDate(tommorrowDate);
                        // const tommorrowDate = new Date(tommorrow);
                        const activationDateInMillis = tommorrowDate.getTime();

                        if (activationDateInMillis < activationRange[0] || activationDateInMillis > activationRange[1]) {
                            errCode = '405';
                            const errMessage = activationDateInMillis < activationRange[0]
                                ? `as start date(${activationStartDate}) has not commenced`: `as last date(${activationEndDate}) has passed`;

                            throw new Error(`Coupon cannot be activated now, ${errMessage}`);
                        }

                        const submissionDate = today;
                        const activationDate = tommorrow;
                        const status = 'ASSIGNED';
                        const start = new Date(startDate);
                        const startDateMillis = start.getTime();

                        let expiryDate;
                        if (offerExpiryType === 'FIXED') {
                            expiryDate = endDate;
                        } else if (offerExpiryType === 'NDAYS') {
                            const expDate = new Date();
                            if (startDateMillis > activationDateInMillis) {
                                expDate.setTime(startDateMillis + (nDays * 86400000));
                            } else {
                                expDate.setTime(activationDateInMillis + (nDays * 86400000));
                            }
                            
                            expiryDate = formatDate(expDate);
                        }

                        // if activationDate(tommorrow) is less than startDate use start date else use activationDate
                        const validity = `${startDateMillis > activationDateInMillis? startDate : activationDate} to ${expiryDate}`;
                        let conditions = '';

                        if (maxDiscount) {
                            conditions = `max discount of Rs ${maxDiscount}`
                        }

                        if (minOrder) {
                            conditions = conditions.length > 0 ? `${conditions}, `: conditions;
                            conditions = `${conditions}mininum ${orderUnit}: ${minOrder}`;
                        }

                        // extract Merchant details
                        const merchantResult = await dynamo.get({
                            TableName: TABLE_NAME,
                            Key: {
                                partitionkey: MERCHANT_PK,
                                sortkey: `${MERCHANT_SK_PREFIX}${merchantId}`,
                            },
                        }).promise();

                        const { name, logoUrl } = merchantResult.Item;

                        let location = isAllLocation ? 'All Locations' : merchantLocationId;

                        if (!isAllLocation) {
                            const merchantLocationResult = await dynamo.get({
                                TableName: TABLE_NAME,
                                Key: {
                                    partitionkey: `${MERCHANT_LOCATION_PK}${merchantId}`,
                                    sortkey: `${MERCHANT_LOCATION_SK_PREFIX}${merchantLocationId}`,
                                },
                            }).promise();

                            location = merchantLocationResult.Item.branch;
                        }
                        await dynamo.put({
                            TableName: TABLE_NAME,
                            Item: {
                                partitionkey: `${USER_COUPON_PK}${phone}`,
                                sortkey: `${CCODE_SK_PREFIX}${code}`,
                                activationDate,
                                submissionDate,
                                desc,
                                status,
                                expiryDate,
                                offerId,
                                validity,
                                conditions,
                                merchantLocationId,
                                name,
                                logoUrl,
                                location
                            }
                        }).promise();

                        // set isAssigned to true in offer coupon
                        const update = await dynamo.update({
                            TableName: TABLE_NAME,
                            Key: {
                                partitionkey: OFFER_COUPON_PK,
                                sortkey: `${COUPON_CODE_SK_PREFIX}${code}`,
                            },
                            UpdateExpression: 'SET isAssigned = :flag',
                            ExpressionAttributeValues: {
                                ':flag': true,
                            },
                            ReturnValues: 'ALL_NEW',
                        }).promise();

                        statusCode = '201';

                        body = {
                            code,
                            desc,
                            validity,
                            conditions,
                            name,
                            logoUrl,
                            location
                        }
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
