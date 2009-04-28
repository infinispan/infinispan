package org.infinispan.loader.s3.jclouds;

import org.infinispan.loader.bucket.Bucket;
import org.infinispan.loader.s3.S3ConnectionException;
import org.jclouds.aws.s3.domain.S3Bucket;
import org.jclouds.aws.s3.domain.S3Object;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * A {@link org.jclouds.aws.s3.S3Connection jclouds} implementation of {@link org.infinispan.loader.s3.S3Bucket}.
 * <p/>
 * Tuning and configuration parameters can be overridden by creating <tt>jets3t.properties</tt> and adding it to your
 * classpath.
 *
 * @author Adrian Cole
 * @link http://jets3t.s3.amazonaws.com/toolkit/configuration.html
 * @since 1.0
 */
public class JCloudsBucket implements org.infinispan.loader.s3.S3Bucket<S3Bucket, org.infinispan.loader.s3.jclouds.JCloudsConnection> {
    private String name;

    public void init(JCloudsConnection connection, S3Bucket bucket) {
        this.connection = connection;
        this.rootS3Bucket = bucket;
        this.name = bucket.getName();
    }

    public String getName() {
        return name;
    }

    private JCloudsConnection connection;
    protected S3Bucket rootS3Bucket;


    /**
     * {@inheritDoc}
     */
    public Bucket get(String key) throws S3ConnectionException {
        Bucket bucket = null;
        InputStream is = null;
        ObjectInputStream ois = null;

        try {
            S3Object s3Object = connection.getConnection().getObject(rootS3Bucket, key).get();
            // it is possible that the object never existed. in this case, fall out.
            if (s3Object != null && s3Object.getContent() != null) {
                is = (InputStream) s3Object.getContent();
                bucket = (Bucket) connection.marshaller.objectFromStream(is);
                // TODO hack until we are sure the bucket has an immutable name
                bucket.setBucketName(key);
            }
        } catch (Exception e) {
            throw connection.convertToS3ConnectionException("Error while reading from object: " + key, e);
        } finally {
            safeClose(ois);
            safeClose(is);
        }
        return bucket;
    }

    /**
     * {@inheritDoc}
     */
    public void remove(String key) throws S3ConnectionException {
        try {
            connection.getConnection().deleteObject(rootS3Bucket, key).get();
        } catch (Exception ex) {
            throw connection.convertToS3ConnectionException("Exception removing key " + key, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Set<String> keySet() throws S3ConnectionException {
        return connection.keysInBucket(rootS3Bucket);
    }

    public Set<Bucket> values() throws S3ConnectionException {
        Set<Bucket> buckets = new HashSet<Bucket>();
        for (String s : keySet()) {
            Bucket bucket = get(s);
            if (bucket != null)
                buckets.add(bucket);
        }
        return buckets;
    }

    /**
     * {@inheritDoc}
     */
    public void clear() throws S3ConnectionException {
        try {
            List<Future<Boolean>> deletes = new ArrayList<Future<Boolean>>();
            for (String key : keySet()) {
                deletes.add(connection.getConnection().deleteObject(rootS3Bucket, key));
            }
            for (Future<Boolean> delete : deletes) {
                if (!delete.get())
                    throw new S3ConnectionException("could not delete entry");
            }
        } catch (Exception ex) {
            throw connection.convertToS3ConnectionException("Exception clearing store", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void insert(Bucket b) throws S3ConnectionException {
        try {
            if (b.getEntries().isEmpty()) {
                if (!connection.getConnection().deleteObject(rootS3Bucket, b.getBucketName()).get())
                    throw new S3ConnectionException(String.format("Could not delete object [%2s] in s3bucket [%1s] ", rootS3Bucket.getName(), b.getBucketName()));
            } else {
                S3Object s3Object = new S3Object();
                s3Object.setKey(b.getBucketName());
                s3Object.setContent(connection.marshaller.objectToByteBuffer(b));
                s3Object.setContentType("application/octet-string");
                String id = connection.getConnection().addObject(rootS3Bucket, s3Object).get();
                assert id != null : String.format("Should have received an id for entry %1s:%2s ", rootS3Bucket.getName(), b.getBucketName());
            }
        } catch (Exception ex) {
            throw connection.convertToS3ConnectionException("Exception while saving bucket " + b, ex);
        }
    }

    protected final void safeClose(InputStream stream) throws S3ConnectionException {
        if (stream == null) return;
        try {
            stream.close();
        } catch (Exception e) {
            throw new S3ConnectionException("Problems closing input stream", e);
        }
    }

}