package org.infinispan.marshall.exts;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.infinispan.commons.marshall.AbstractExternalizer;

public abstract class AbstractExternalizerTest<T> {

    protected AbstractExternalizer<T> externalizer;

    public AbstractExternalizerTest() {
        this.externalizer = createExternalizer();
    }

    protected T deserialize(T object) throws IOException, ClassNotFoundException {
        byte[] buffer;
        final int byteArrayLength = 8 * 6; // 8 bytes (size of long/double) * 6 fields
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(byteArrayLength)) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                externalizer.writeObject(oos, object);
            }
            buffer = baos.toByteArray();
        }
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buffer));
        return externalizer.readObject(ois);
    }

    abstract AbstractExternalizer<T> createExternalizer();
}
