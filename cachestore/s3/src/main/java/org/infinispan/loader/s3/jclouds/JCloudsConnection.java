package org.infinispan.loader.s3.jclouds;

import org.infinispan.loader.s3.S3CacheStoreConfig;
import org.infinispan.loader.s3.S3Connection;
import org.infinispan.loader.s3.S3ConnectionException;
import org.infinispan.marshall.Marshaller;
import org.jclouds.aws.s3.S3ConnectionFactory;
import org.jclouds.aws.s3.nio.config.S3HttpNioConnectionPoolClientModule;
import org.jclouds.aws.s3.domain.S3Bucket;
import org.jclouds.aws.s3.domain.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * // TODO: Adrian: Document this!
 *
 * @author Adrian Cole
 */
public class JCloudsConnection implements S3Connection<org.jclouds.aws.s3.S3Connection, org.jclouds.aws.s3.domain.S3Bucket> {
    protected org.jclouds.aws.s3.S3Connection s3Service;
    protected Marshaller marshaller;

    /**
     * {@inheritDoc}
     */
    public void connect(S3CacheStoreConfig config, Marshaller m) throws S3ConnectionException {
        //TODO max connections
        //TODO proxy host/port
        InputStream propertiesIS = null;
        try {
            propertiesIS = JCloudsConnection.class.getResourceAsStream("/jclouds.properties");
            Properties properties = new Properties();
            properties.load(propertiesIS);
            if (!config.isSecure()) {
                properties.put("jclouds.http.port", "80");
                properties.put("jclouds.http.secure", "false");
            }
            if (!properties.containsKey("jclouds.aws.accesskeyid"))
                properties.put("jclouds.aws.accesskeyid", config.getAwsAccessKey());
            if (!properties.containsKey("jclouds.aws.secretaccesskey"))
                properties.put("jclouds.aws.secretaccesskey", config.getAwsSecretKey());
            this.s3Service = S3ConnectionFactory.getConnection(properties, new S3HttpNioConnectionPoolClientModule());
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
     * @see //TODO org.jets3t.service.S3Service#getOrCreateBucket(String)
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
            for (S3Object object : s3Service.getBucket(bucket).get().getContents()) {
                s3Service.deleteObject(bucket, object.getKey());
            }
            s3Service.deleteBucket(bucket);
        } catch (Exception ex) {
            throw convertToS3ConnectionException("Exception removing s3 bucket " + name, ex);
        }
    }


    Set<String> keysInBucket(S3Bucket bucket) throws S3ConnectionException {
        try {
            Set<String> keys = new HashSet<String>();
            for (S3Object object : s3Service.getBucket(bucket).get().getContents()) {
                keys.add(object.getKey());
            }
            return keys;
        } catch (Exception ex) {
            throw convertToS3ConnectionException("Exception while listing bucket " + bucket, ex);
        }
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


    public void disconnect() {
        try {
            s3Service.close();
        } catch (IOException e) {
            e.printStackTrace();  // TODO: Adrian: Customise this generated block
        }
    }

    S3ConnectionException convertToS3ConnectionException(String message, Exception caught) {
        return (caught instanceof S3ConnectionException) ? (S3ConnectionException) caught :
                new S3ConnectionException(message, caught);
    }
}
