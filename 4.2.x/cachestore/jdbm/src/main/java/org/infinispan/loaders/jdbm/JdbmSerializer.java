package org.infinispan.loaders.jdbm;

import java.io.IOException;

import org.infinispan.marshall.StreamingMarshaller;

import jdbm.helper.Serializer;

/**
 * Uses the configured (runtime) {@link org.infinispan.marshall.StreamingMarshaller} of the cache.
 * This Serializer is thus not really serializable.
 * 
 * @author Elias Ross
 */
@SuppressWarnings("serial")
public class JdbmSerializer implements Serializer {
    
    private transient StreamingMarshaller marshaller;

    /**
     * Constructs a new JdbmSerializer.
     */
    public JdbmSerializer(StreamingMarshaller marshaller) {
        if (marshaller == null)
            throw new NullPointerException("marshaller");
        this.marshaller = marshaller;
    }

    public Object deserialize(byte[] buf) throws IOException {
        try {
            return marshaller.objectFromByteBuffer(buf);
        } catch (ClassNotFoundException e) {
            throw (IOException)new IOException().initCause(e);
        }
    }

    public byte[] serialize(Object obj) throws IOException {
        return marshaller.objectToByteBuffer(obj);
    }

}
