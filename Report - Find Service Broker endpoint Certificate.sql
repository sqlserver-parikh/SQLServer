--http://rusanu.com/2008/10/25/replacing-endpoint-certificates-that-are-near-expiration/

SELECT BE.name ServiceBrokerEndPoint,
       BE.type_desc TypeDesc,
       MC.name CertName,
       MC.expiry_date ExpiryDate,
       MC.thumbprint ThumbPrint,
       pvt_key_encryption_type_desc PVTEncryption,
       MC.subject CertSubject
FROM sys.service_broker_endpoints BE
     INNER JOIN master.sys.certificates MC ON BE.certificate_id = MC.certificate_id;
