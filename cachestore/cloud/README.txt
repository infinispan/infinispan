 mvn -Pintegration test -Dinfinispan.jclouds.username=$S3_USER -Dinfinispan.jclouds.password=$S3_PWD -Dinfinispan.jclouds.service=s3
 mvn -Pintegration test -Dinfinispan.jclouds.username=$RACKSPACE_USER -Dinfinispan.jclouds.password=$RACKSPACE_PWD -Dinfinispan.jclouds.service=cloudfiles
 mvn -Pintegration test -Dinfinispan.jclouds.username=$AZURE_USER -Dinfinispan.jclouds.password=$AZURE_PWD -Dinfinispan.jclouds.service=azureblob
