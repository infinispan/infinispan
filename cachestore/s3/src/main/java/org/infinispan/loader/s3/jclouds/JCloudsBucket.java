package org.infinispan.loader.s3.jclouds;

import org.infinispan.loader.bucket.Bucket;
import org.infinispan.loader.s3.S3ConnectionException;
import org.jclouds.aws.s3.domain.S3Bucket;
import org.jclouds.aws.s3.domain.S3Object;
import org.jclouds.aws.s3.S3ObjectMap;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link org.jclouds.aws.s3.S3Connection JClouds} implementation of {@link org.infinispan.loader.s3.S3Bucket}.
 * <p/>
 * Tuning and configuration parameters can be overridden by creating <tt>jclouds.properties</tt> and adding it to your
 * classpath.
 *
 * @author Adrian Cole
 * @link http://code.google.com/p/jclouds
 * @since 4.0
 */
public class JCloudsBucket implements org.infinispan.loader.s3.S3Bucket<S3Bucket, org.infinispan.loader.s3.jclouds.JCloudsConnection> {

    private JCloudsConnection connection;
    private String name;
    private S3Bucket rootS3Bucket;
    private Map<String, InputStream> map;

    public void init(JCloudsConnection connection, S3Bucket bucket) {
        this.connection = connection;
        this.rootS3Bucket = bucket;
        this.name = bucket.getName();
        this.map = connection.context.createMapView(rootS3Bucket);
    }

    public String getName() {
        return name;
    }


    /**
     * {@inheritDoc}
     */
    public Bucket get(String key) throws S3ConnectionException {
        InputStream input = null;
        try {
            input = map.get(key);
            // it is possible that the object never existed. in this case, fall out.
            if (input != null) {
                return bucketFromStream(key, input);
            }
            return null;
        } catch (Exception e) {
            throw connection.convertToS3ConnectionException("Error while reading from object: " + key, e);
        } finally {
            safeClose(input);
        }
    }

    private Bucket bucketFromStream(String key, InputStream input) throws S3ConnectionException {
        try {
            Bucket bucket = (Bucket) connection.marshaller.objectFromStream(input);
            // TODO hack until we are sure the bucket has an immutable name
            bucket.setBucketName(key);
            return bucket;
        } catch (Exception e) {
            throw connection.convertToS3ConnectionException("Error while reading from object: " + key, e);
        } finally {
            safeClose(input);
        }
    }

    /**
     * {@inheritDoc}
     */

    public void remove(String key) throws S3ConnectionException {
        try {
            map.remove(key);
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
        for (Map.Entry<String, InputStream> entry : map.entrySet()) {
            buckets.add(bucketFromStream(entry.getKey(), entry.getValue()));
        }
        return buckets;
    }

    /**
     * {@inheritDoc}
     */
    public void clear() throws S3ConnectionException {
        try {
            map.clear();
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
                map.remove(b.getBucketName());
            } else {
                ((S3ObjectMap)map).putBytes(b.getBucketName(),connection.marshaller.objectToByteBuffer(b));
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