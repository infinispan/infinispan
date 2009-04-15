package org.infinispan.loader.s3;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loader.CacheLoaderConfig;
import org.infinispan.loader.CacheLoaderException;
import org.infinispan.loader.bucket.Bucket;
import org.infinispan.loader.bucket.BucketBasedCacheStore;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.marshall.Marshaller;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * A TODO link implementation of a {@link org.infinispan.loader.bucket.BucketBasedCacheStore}.
 * This file store stores stuff in the following format: <tt>http://s3.amazon.com/{bucket}/bucket_number.bucket</tt>
 * <p/>
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class S3CacheStore extends BucketBasedCacheStore {

    private static final Log log = LogFactory.getLog(S3CacheStore.class);

    private S3CacheStoreConfig config;

    Cache cache;
    Marshaller marshaller;

    private S3Connection connection;
    private S3Bucket s3Bucket;

    public Class<? extends CacheLoaderConfig> getConfigurationClass() {
        return S3CacheStoreConfig.class;
    }

    /**
     * {@inheritDoc} This initializes the internal <tt>s3Connection</tt> to a default implementation
     */
    public void init(CacheLoaderConfig config, Cache cache, Marshaller m) {
        throw new UnsupportedOperationException("no default implementation, yet");
//        init(config, cache, m, null, null);
    }

    @Override
    public void stop() throws CacheLoaderException {
        super.stop();
        this.connection.disconnect();
    }

    public void init(CacheLoaderConfig config, Cache cache, Marshaller m, S3Connection connection, S3Bucket bucket) {
        super.init(config, cache, m);
        this.config = (S3CacheStoreConfig) config;
        this.cache = cache;
        this.marshaller = m;
        this.connection = connection;
        this.s3Bucket = bucket;
    }


    public void start() throws CacheLoaderException {
        super.start();

        if (config.getAwsAccessKey() == null)
            throw new IllegalArgumentException("awsAccessKey must be set");
        if (config.getAwsSecretKey() == null)
            throw new IllegalArgumentException("awsSecretKey must be set");
        this.connection.connect(config, marshaller);
        String s3Bucket = config.getBucket();
        if (s3Bucket == null)
            throw new IllegalArgumentException("s3Bucket must be set");
        this.s3Bucket.init(this.connection, connection.verifyOrCreateBucket(s3Bucket));
    }

    protected Set<InternalCacheEntry> loadAllLockSafe() throws CacheLoaderException {
        Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
        // TODO I don't know why this returns objects at the moment
        for (Bucket bucket : (Set<Bucket>) s3Bucket.values()) {
            if (bucket.removeExpiredEntries()) {
                saveBucket(bucket);
            }
            result.addAll(bucket.getStoredEntries());
        }
        return result;
    }

    protected void fromStreamLockSafe(ObjectInput objectInput) throws CacheLoaderException {
        String source;
        try {
            source = (String) objectInput.readObject();

        } catch (Exception e) {
            throw convertToCacheLoaderException("Error while reading from stream", e);
        }
        if (config.getBucket().equals(source)) {
            log.info("Attempt to load the same s3 bucket ignored");
        } else {
            connection.copyBucket(source, config.getBucket());
        }
    }

    protected void toStreamLockSafe(ObjectOutput objectOutput) throws CacheLoaderException {
        try {
            objectOutput.writeObject(config.getBucket());
        } catch (IOException e) {
            throw convertToCacheLoaderException("Error while writing to stream", e);
        }
    }

    protected void clearLockSafe() throws CacheLoaderException {
        s3Bucket.clear();
    }

    CacheLoaderException convertToCacheLoaderException(String message, Exception caught) {
        return (caught instanceof CacheLoaderException) ? (CacheLoaderException) caught :
                new CacheLoaderException(message, caught);
    }

    protected void purgeInternal() throws CacheLoaderException {
        loadAll();
    }

    protected Bucket loadBucket(String bucketName) throws CacheLoaderException {
        return s3Bucket.get(bucketName);
    }


    protected void insertBucket(Bucket bucket) throws CacheLoaderException {
        s3Bucket.insert(bucket);
    }

    protected void saveBucket(Bucket bucket) throws CacheLoaderException {
        s3Bucket.insert(bucket);
    }


}
