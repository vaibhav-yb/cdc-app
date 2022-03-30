package org.vaibhav;

public class UtilStrings {
    public static String apiAppleStoreReceiptLog = "insert into api_db_domestic.api_apple_store_receipt_log (id, appitemid, bid, bvrs, expiresdate, expiresdateformated,"
            + " expiresdateformattedpst, itemid, "
            + "originalpurchasedate, originalpurchasedatems, originalpurchasedatepst, originaltransactionid, productid, purchasedate, purchasedatems, purchasedatepst, "
            + "quantity, transactionid, uniqueidentifier, userid, versionexternalidentifier, weborderlineitemid, expirationintent, cancellationreason) values (%d, "
            + "'fa4323d6-8293-46e6-b0eb-a080cfeb04cf', '917e7638-ea19-4d55-91a9-d41cfd3de13a', 'Portuguese', '2010-06-23T12:53:53Z', 'YYYY-MM-ddTHH:mm:ssZ', 'YYYY/dd/mm', '1f17b20c-cf72-44c2-99ae-eac91bb15c97', '2021-05-18T17:03:32Z', 'YYYY-MM-ddTHH:mm:ssZ', '2010-09-16', '7296ec22-3ef7-4474-b7e5-df720ec1bb5e', '1G6AF5S36E0511228', '2019-11-18T15:44:46Z', 'YYYY-MM-ddTHH:mm:ssZ', '2012-02-28', 67, '45b08c1d-f9ef-4127-80c8-a65f049713d2', 'd511accb-99f1-4397-9693-ca6802da2760', 260036, 'fe6bde4f-69c6-406c-91ee-e24f281535df', '00cb6627-91c7-4aa3-ad2d-ead96011eb5c', 21, 84);";

    public static String apiOauthAccessToken = "insert into api_db_domestic.api_oauth_access_token (access_token_id, access_token, access_token_hashed,"
            + "additional_information, client_id, expires_in, grant_type, refresh_token, refresh_token_hashed, scope, userid) values (%d, "
            + "'15eujNJCmhVdqFN5T3dPkGEh6hfo3kTbSn', '8bad98be96213362ff4aa8c638f0e0c4c1aa2ba032c86de7ceab2c38d9d1986f', 'Curabitur at ipsum ac tellus semper interdum.', 'f0b015f4-673f-4ca3-9e2a-ecc0aec3d9c7', '2013-11-16T04:40:29Z', 56, '1FhDfbBiBVXWGZfXTG6AhYEtte7CDmpd4y', '30799c54ca6547a63a5cec4afe2a294c039a028e46038e93d6dcd1bbdda62c4f', 95, 139238);";

    public static String apiSubAppleOrigTransactions = "insert into api_db_domestic.api_sub_apple_orig_transactions (id, createddate, lastcheckeddate, "
            + "originaltransactionid, receipttext, status) values (%d, '2016-07-14T09:39:04Z', '2019-02-20T13:44:56Z', '39e64acc-1c47-434a-9066-96abe8ba7add', 'Azeri', "
            + "79);";

    public static String apiSubRecurlyNotifyLog = "insert into api_db_domestic.api_sub_recurly_notify_log (id, ingestdate, notification, subscriptionid, userid) values"
            + " (%d, '2020-09-03T08:21:22Z', 'Polarised multi-state interface', 1307, 324586);";
}
