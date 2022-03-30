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
  id             bigint not null,
  ingestdate     timestamptz,
  notification   varchar(255),
  subscriptionid varchar(255),
  userid         bigint
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_sub_user_packages CASCADE;
CREATE TABLE api_db_domestic.api_sub_user_packages
(
  id                                  bigint not null,
  createddate                         timestamptz,
  enddate                             timestamptz,
  packagecode                         varchar(255),
  pspmodifieddate                     timestamptz,
  state                               bigint,
  updateddate                         timestamptz,
  userid                              bigint,
  source                              varchar(255),
  couponused                          varchar(255),
  isstateactive                       bigint,
  cbsproductcode                      varchar(255),
  vendororiginalpurchasetransactionid varchar(255),
  vendorsuppliedid                    varchar(255),
  lastbillingvendorsynctimestamp      bigint,
  expirationintent                    varchar(255),
  renewstatus                         varchar(255),
  retryvalue                          varchar(255),
  cancelreason                        varchar(255),
  product_region                      varchar(255),
  startdate                           timestamptz,
  vendorcode                          varchar(255)
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_user_partner_transaction_log CASCADE;
CREATE TABLE api_db_domestic.api_user_partner_transaction_log
(
  id                    bigint not null,
  isoriginalpurchase    boolean,
  json                  text,
  originaltransactionid varchar(255),
  partner               varchar(255),
  transactionid         varchar(255),
  transactiontype       bigint,
  userid                bigint
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_user_device CASCADE;
CREATE TABLE api_db_domestic.api_user_device
(
  id                bigint not null,
  activationcode    varchar(255),
  createddate       timestamptz,
  devicedescription varchar(255),
  deviceid          varchar(255),
  devicetoken       varchar(255),
  ipaddress         varchar(255),
  partner           varchar(255),
  lastupdate        timestamptz,
  status            bigint,
  userid            bigint,
  encryption_level  bigint,
  isdeleted         boolean
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.api_watch_list CASCADE;
CREATE TABLE api_db_domestic.api_watch_list
(
  id                      bigint not null,
  userid                  bigint not null default '0',
  profileid               bigint not null default '0',
  externalid              varchar(255) not null,
  watchlistexternalidtype varchar(255),
  createddate             timestamptz,
  updateddate             timestamptz
) split into 40 tablets;

DROP TABLE IF EXISTS api_db_domestic.user_attribute CASCADE;
CREATE TABLE api_db_domestic.user_attribute
(
  userid                              bigint not null,
  lastshareddate                      timestamptz,
  optin                               boolean,
  opt_in_updated_date                 timestamptz,
  sharestatus                         char(1),
  termsofusedate                      date,
  termsofuseversion                   varchar(255),
  userstatus                          bigint,
  verifiedemail                       boolean,
  parental_control_pin                varchar(10),
  parental_control_restriction_level  varchar(100),
  parental_control_livetv_pin_enabled boolean,
  createddate                         timestamptz,
  updateddate                         timestamptz,
  nfloptin                            boolean,
  nfloptindate                        timestamptz
) split into 40 tablets;