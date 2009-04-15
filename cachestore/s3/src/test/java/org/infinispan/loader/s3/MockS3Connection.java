package org.infinispan.loader.s3;

import org.infinispan.loader.bucket.Bucket;
import org.infinispan.marshall.Marshaller;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores S3 Buckets in a map instead of connecting to a live server.
 *
 * @since 4.0
 * @author Adrian Cole
 */
public class MockS3Connection implements S3Connection<MockS3Connection, Map<String, Bucket>> {
    private static Map<String, Map<String, Bucket>> bucketToContents = new ConcurrentHashMap<String, Map<String, Bucket>>();

    public void connect(S3CacheStoreConfig config, Marshaller m) throws S3ConnectionException {
        // Do nothing
    }

    public MockS3Connection getConnection() throws S3ConnectionException {
        return this;
    }

    public Map<String, Bucket> verifyOrCreateBucket(String bucketName) throws S3ConnectionException {
        if (!bucketToContents.containsKey(bucketName)) {
            bucketToContents.put(bucketName, new ConcurrentHashMap<String, Bucket>());
        }
        return bucketToContents.get(bucketName);
    }

    public void destroyBucket(String name) throws S3ConnectionException {
        bucketToContents.remove(name);
    }

    public void disconnect() {
        // na
    }

    public void copyBucket(String sourceBucket, String destinationBucket) throws S3ConnectionException {
        Map<String, Bucket> source = bucketToContents.get(sourceBucket);
        Map<String, Bucket> dest = bucketToContents.get(destinationBucket);
        for (Bucket bucket : source.values()) {
            dest.put(bucket.getBucketName(), bucket);
        }
    }
}
