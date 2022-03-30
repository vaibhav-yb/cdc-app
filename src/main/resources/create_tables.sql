DROP SCHEMA api_db_domestic CASCADE;
CREATE SCHEMA api_db_domestic;

DROP TABLE IF EXISTS api_db_domestic.api_apple_store_receipt_log CASCADE;
CREATE TABLE api_db_domestic.api_apple_store_receipt_log
(
  id                        bigint not null,
  appitemid                 varchar(255),
  bid                       varchar(255),
  bvrs                      varchar(255),
  expiresdate               varchar(255),
  expiresdateformated       varchar(255),
  expiresdateformattedpst   varchar(255),
  itemid                    varchar(255),
  originalpurchasedate      varchar(255),
  originalpurchasedatems    varchar(255),
  originalpurchasedatepst   varchar(255),
  originaltransactionid     varchar(255),
  productid                 varchar(255),
  purchasedate              varchar(255),
  purchasedatems            varchar(255),
  purchasedatepst           varchar(255),
  quantity                  bigint not null,
  transactionid             varchar(255),
  uniqueidentifier          varchar(255),
  userid                    bigint,
  versionexternalidentifier varchar(255),
  weborderlineitemid        varchar(255),
  expirationintent          bigint,
  cancellationreason        bigint
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_oauth_access_token CASCADE;
CREATE TABLE api_db_domestic.api_oauth_access_token
(
  access_token_id        bigint not null,
  access_token           varchar(2048) not null,
  access_token_hashed    varchar(512) not null,
  additional_information varchar(1024),
  client_id              varchar(255) not null,
  expires_in             timestamptz not null,
  grant_type             bigint not null,
  refresh_token          varchar(2048) not null,
  refresh_token_hashed   varchar(512) not null,
  scope                  bigint,
  userid                 bigint not null
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_sub_apple_orig_transactions CASCADE;
CREATE TABLE api_db_domestic.api_sub_apple_orig_transactions
(
  id                    bigint not null,
  createddate           timestamptz,
  lastcheckeddate       timestamptz,
  originaltransactionid varchar(255),
  receipttext           text,
  status                bigint
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_sub_recurly_notify_log CASCADE;
CREATE TABLE api_db_domestic.api_sub_recurly_notify_log
(
  id             bigserial not null,
  ingestdate     timestamptz,
  notification   varchar(255),
  subscriptionid varchar(255),
  userid         bigint
) split into 40 tablets;