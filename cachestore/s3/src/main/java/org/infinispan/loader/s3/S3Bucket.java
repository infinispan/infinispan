package org.infinispan.loader.s3;

import org.infinispan.loader.bucket.Bucket;

import java.util.Set;

/**
 * // TODO: Adrian: Document this!
 * <p/>
 * This interface defines the interactons between the {@link S3CacheStore} and Amazon S3.
 *
 * @author Adrian Cole
 * @since 4.0
 */
public interface S3Bucket<B, C extends S3Connection> {

    void init(C connection, B bucket);

    String getName();

    void insert(Bucket object) throws S3ConnectionException;

    Bucket get(String key) throws S3ConnectionException;

    Set<String> keySet() throws S3ConnectionException;

    Set<Bucket> values() throws S3ConnectionException;

    public void remove(String key) throws S3ConnectionException;

    void clear() throws S3ConnectionException;

}