package org.infinispan.loader.s3.jclouds;

import org.infinispan.loader.s3.S3CacheStoreConfig;
import org.infinispan.loader.s3.S3Connection;
import org.infinispan.loader.s3.S3ConnectionException;
import org.infinispan.marshall.Marshaller;
import org.jclouds.aws.s3.S3Constants;
import org.jclouds.aws.s3.S3Context;
import org.jclouds.aws.s3.S3ContextFactory;
import org.jclouds.aws.s3.domain.S3Bucket;
import org.jclouds.aws.s3.domain.S3Object;
import org.jclouds.aws.s3.nio.config.S3HttpNioConnectionPoolClientModule;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

/**
 * An JClouds implementation of {@link S3Connection}.  This implementation uses the threadsafe {@link S3HttpNioConnectionPoolClientModule} transport.
 *
 * @author Adrian Cole
 * @link http://code.google.com/p/jclouds
 */
public class JCloudsConnection implements S3Connection<org.jclouds.aws.s3.S3Connection, org.jclouds.aws.s3.domain.S3Bucket> {
    protected org.jclouds.aws.s3.S3Connection s3Service;
    protected S3Context context;
    protected Marshaller marshaller;

    /**
     * {@inheritDoc}
     */
    public void connect(S3CacheStoreConfig config, Marshaller m) throws S3ConnectionException {
        InputStream propertiesIS = null;
        try {
            propertiesIS = JCloudsConnection.class.getResourceAsStream("/jclouds.properties");
            Properties properties = new Properties();
            properties.load(propertiesIS);
            if (!config.isSecure()) {
                properties.put(S3Constants.PROPERTY_HTTP_PORT, "80");
                properties.put(S3Constants.PROPERTY_HTTP_SECURE, "false");
            }
            if (!properties.containsKey(S3Constants.PROPERTY_AWS_ACCESSKEYID))
                properties.put(S3Constants.PROPERTY_AWS_ACCESSKEYID, config.getAwsAccessKey());
            if (!properties.containsKey(S3Constants.PROPERTY_AWS_SECRETACCESSKEY))
                properties.put(S3Constants.PROPERTY_AWS_SECRETACCESSKEY, config.getAwsSecretKey());
            if (!properties.containsKey(S3Constants.PROPERTY_POOL_MAX_CONNECTIONS))
                properties.put(S3Constants.PROPERTY_POOL_MAX_CONNECTIONS, config.getMaxConnections());
            //TODO proxy host/port
            this.context = S3ContextFactory.createS3Context(properties, new S3HttpNioConnectionPoolClientModule());
            this.s3Service = context.getConnection();
            if (this.s3Service == null) {
                throw new S3ConnectionException("Could not connect");
            }

        } catch (Exception ex) {
            throw convertToS3ConnectionException("Exception connecting to s3", ex);
        }
        this.marshaller = m;
    }

    public org.jclouds.aws.s3.S3Connection getConnection() throws S3ConnectionException {
        return s3Service;
    }

    /**
     * @see
     */
    public org.jclouds.aws.s3.domain.S3Bucket verifyOrCreateBucket(String bucketName) throws S3ConnectionException {
        try {
            org.jclouds.aws.s3.domain.S3Bucket bucket = new org.jclouds.aws.s3.domain.S3Bucket();
            bucket.setName(bucketName);
            s3Service.createBucketIfNotExists(bucket).get();
            return bucket;
        } catch (Exception ex) {
            throw convertToS3ConnectionException("Exception retrieving or creating s3 bucket " + bucketName, ex);
        }
    }

    public void destroyBucket(String name) throws S3ConnectionException {
        try {
            org.jclouds.aws.s3.domain.S3Bucket bucket = new org.jclouds.aws.s3.domain.S3Bucket();
            bucket.setName(name);
            context.createMapView(bucket).clear();
            s3Service.deleteBucket(bucket);
        } catch (Exception ex) {
            throw convertToS3ConnectionException("Exception removing s3 bucket " + name, ex);
        }
    }


    Set<String> keysInBucket(S3Bucket bucket) throws S3ConnectionException {
        return context.createMapView(bucket).keySet();
    }

    /**
     * {@inheritDoc}
     */
    public void copyBucket(String sourceBucket, String destinationBucket) throws S3ConnectionException {
        Set<String> sourceKeys = null;
        try {
            S3Bucket source = new S3Bucket();
            source.setName(sourceBucket);
            source = s3Service.getBucket(source).get();
            sourceKeys = keysInBucket(source);
            S3Bucket dest = new S3Bucket();
            dest.setName(destinationBucket);

            for (String key : sourceKeys) {
                try {
                    S3Object object = new S3Object();
                    object.setKey(key);
                    s3Service.copyObject(source, object, dest, object).get();
                } catch (Exception ex) {
                    throw convertToS3ConnectionException("Exception while copying key " + key + " from bucket " + sourceBucket, ex);
                }
            }
        } catch (Exception ex) {
            throw convertToS3ConnectionException("Cannot access bucket " + sourceBucket, ex);
        }

    }

    /**
     * @see S3Context#close
     */
    public void disconnect() {
        context.close();
    }

    S3ConnectionException convertToS3ConnectionException(String message, Exception caught) {
        return (caught instanceof S3ConnectionException) ? (S3ConnectionException) caught :
                new S3ConnectionException(message, caught);
    }
}
