 mvn test -Dtest=CloudCacheStoreFunctionalIntegrationTest -Dinfinispan.jclouds.username=$S3_USER -Dinfinispan.jclouds.password=$S3_PWD -Dinfinispan.jclouds.service=s3
 mvn test -Dtest=CloudCacheStoreFunctionalIntegrationTest -Dinfinispan.jclouds.username=$RACKSPACE_USER -Dinfinispan.jclouds.password=$RACKSPACE_PWD -Dinfinispan.jclouds.service=cloudfiles
 mvn test -Dtest=CloudCacheStoreFunctionalIntegrationTest -Dinfinispan.jclouds.username=$AZURE_USER -Dinfinispan.jclouds.password=$AZURE_PWD -Dinfinispan.jclouds.service=azureblob
