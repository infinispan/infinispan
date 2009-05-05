package org.infinispan.loaders.s3.jclouds;

import org.infinispan.loaders.s3.S3CacheStoreConfig;
import org.infinispan.loaders.s3.S3Connection;
import org.infinispan.loaders.s3.S3ConnectionException;
import org.infinispan.marshall.Marshaller;
import org.jclouds.aws.s3.S3Constants;
import org.jclouds.aws.s3.S3Context;
import org.jclouds.aws.s3.S3ContextFactory;
import org.jclouds.aws.s3.domain.S3Bucket;
import org.jclouds.aws.s3.domain.S3Object;
import org.jclouds.aws.s3.nio.config.S3HttpNioConnectionPoolClientModule;
import org.jclouds.logging.jdk.config.JDKLoggingModule;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import com.google.inject.Module;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An JClouds implementation of {@link S3Connection}. This implementation uses
 * the threadsafe {@link S3HttpNioConnectionPoolClientModule} transport.
 * 
 * @author Adrian Cole
 * @link http://code.google.com/p/jclouds
 */
public class JCloudsConnection
	implements
	S3Connection<org.jclouds.aws.s3.S3Connection, org.jclouds.aws.s3.domain.S3Bucket> {
    protected org.jclouds.aws.s3.S3Connection s3Service;
    protected S3Context context;
    protected Marshaller marshaller;
    protected S3CacheStoreConfig config;

    /**
     * {@inheritDoc}
     */
    public void connect(S3CacheStoreConfig config, Marshaller m)
	    throws S3ConnectionException {
	this.config = config;
	InputStream propertiesIS;
	try {
	    propertiesIS = JCloudsConnection.class
		    .getResourceAsStream("/jclouds.properties");
	    Properties properties = new Properties();
	    properties.load(propertiesIS);
	    if (!config.isSecure()) {
		properties.setProperty(S3Constants.PROPERTY_HTTP_PORT, "80");
		properties.setProperty(S3Constants.PROPERTY_HTTP_SECURE,
			"false");
	    }
	    if (properties.containsKey(S3Constants.PROPERTY_AWS_MAP_TIMEOUT)) {
		config.setRequestTimeout(Long.parseLong(properties
			.getProperty(S3Constants.PROPERTY_AWS_MAP_TIMEOUT)));
	    } else {
		properties.setProperty(S3Constants.PROPERTY_AWS_MAP_TIMEOUT,
			config.getRequestTimeout() + "");
	    }
	    if (!properties.containsKey(S3Constants.PROPERTY_AWS_ACCESSKEYID))
		properties.setProperty(S3Constants.PROPERTY_AWS_ACCESSKEYID,
			checkNotNull(config.getAwsAccessKey(),
				"config.getAwsAccessKey()"));
	    if (!properties
		    .containsKey(S3Constants.PROPERTY_AWS_SECRETACCESSKEY))
		properties.setProperty(
			S3Constants.PROPERTY_AWS_SECRETACCESSKEY, checkNotNull(
				config.getAwsSecretKey(),
				"config.getAwsSecretKey()"));
	    if (!properties
		    .containsKey(S3Constants.PROPERTY_POOL_MAX_CONNECTIONS))
		properties.setProperty(
			S3Constants.PROPERTY_POOL_MAX_CONNECTIONS, config
				.getMaxConnections()
				+ "");
	    // TODO proxy host/port
	    Module loggingModule = org.infinispan.logging.LogFactory.IS_LOG4J_AVAILABLE ? new Log4JLoggingModule()
		    : new JDKLoggingModule();
	    this.context = S3ContextFactory.createS3Context(properties,
		    new S3HttpNioConnectionPoolClientModule(), loggingModule);
	    this.s3Service = context.getConnection();
	    if (this.s3Service == null) {
		throw new S3ConnectionException("Could not connect");
	    }

	} catch (Exception ex) {
	    throw convertToS3ConnectionException("Exception connecting to s3",
		    ex);
	}
	this.marshaller = m;
    }

    public org.jclouds.aws.s3.S3Connection getConnection()
	    throws S3ConnectionException {
	return s3Service;
    }

    /**
     * @see
     */
    public org.jclouds.aws.s3.domain.S3Bucket verifyOrCreateBucket(
	    String bucketName) throws S3ConnectionException {
	try {
	    org.jclouds.aws.s3.domain.S3Bucket bucket = new org.jclouds.aws.s3.domain.S3Bucket(
		    bucketName);
	    s3Service.createBucketIfNotExists(bucket).get(
		    config.getRequestTimeout(), TimeUnit.MILLISECONDS);
	    return bucket;
	} catch (Exception ex) {
	    throw convertToS3ConnectionException(
		    "Exception retrieving or creating s3 bucket " + bucketName,
		    ex);
	}
    }

    public void destroyBucket(String name) throws S3ConnectionException {
	try {
	    org.jclouds.aws.s3.domain.S3Bucket bucket = new org.jclouds.aws.s3.domain.S3Bucket(
		    name);
	    context.createS3ObjectMap(bucket).clear();
	    s3Service.deleteBucket(bucket);
	} catch (Exception ex) {
	    throw convertToS3ConnectionException(
		    "Exception removing s3 bucket " + name, ex);
	}
    }

    Set<String> keysInBucket(S3Bucket bucket) throws S3ConnectionException {
	return context.createS3ObjectMap(bucket).keySet();
    }

    /**
     * {@inheritDoc}
     */
    public void copyBucket(String sourceBucket, String destinationBucket)
	    throws S3ConnectionException {
	Set<String> sourceKeys;
	try {
	    S3Bucket source = new S3Bucket(sourceBucket);
	    source = s3Service.getBucket(source).get(
		    config.getRequestTimeout(), TimeUnit.MILLISECONDS);
	    sourceKeys = keysInBucket(source);
	    S3Bucket dest = new S3Bucket(destinationBucket);

	    for (String key : sourceKeys) {
		try {
		    S3Object object = new S3Object(key);
		    s3Service.copyObject(source, object, dest, object).get(
			    config.getRequestTimeout(), TimeUnit.MILLISECONDS);
		} catch (Exception ex) {
		    throw convertToS3ConnectionException(
			    "Exception while copying key " + key
				    + " from bucket " + sourceBucket, ex);
		}
	    }
	} catch (Exception ex) {
	    throw convertToS3ConnectionException("Cannot access bucket "
		    + sourceBucket, ex);
	}

    }

    /**
     * @see S3Context#close
     */
    public void disconnect() {
	context.close();
    }

    S3ConnectionException convertToS3ConnectionException(String message,
	    Exception caught) {
	return (caught instanceof S3ConnectionException) ? (S3ConnectionException) caught
		: new S3ConnectionException(message, caught);
    }
}
