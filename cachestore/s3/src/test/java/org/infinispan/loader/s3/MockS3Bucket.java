package org.infinispan.loader.s3;

import org.infinispan.loader.bucket.Bucket;
import org.infinispan.marshall.Marshaller;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Stores Infinispan Buckets in a map instead of a live S3 Bucket.
 *
 * @author Adrian Cole
 * @version $Id: $
 * @since 4.0
 */
public class MockS3Bucket implements S3Bucket<Map<String, Bucket>, org.infinispan.loader.s3.MockS3Connection> {
    private String name;
    private Map<String, Bucket> s3Bucket;

    public void init(String name, String awsAccessKey, String awsSecretKey, Marshaller m) throws S3ConnectionException {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Set<Bucket> values() throws S3ConnectionException {
        return new HashSet<Bucket>(s3Bucket.values());
    }

    public void init(org.infinispan.loader.s3.MockS3Connection connection, Map<String, Bucket> bucket) {
        this.s3Bucket = bucket;
    }

    public void insert(Bucket object) throws S3ConnectionException {
        s3Bucket.put(object.getBucketName(), object);
    }

    public Bucket get(String key) throws S3ConnectionException {
        return s3Bucket.get(key);
    }

    public Set<String> keySet() throws S3ConnectionException {
        return s3Bucket.keySet();
    }

    public void remove(String key) throws S3ConnectionException {
        s3Bucket.remove(key);
    }

    public void clear() throws S3ConnectionException {
        s3Bucket.clear();
    }

}

