package org.infinispan.commons.marshall;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;

/**
 * Standard Java serialization marshaller.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JavaSerializationMarshaller extends AbstractMarshaller {

   final ClassAllowList allowList;

   public JavaSerializationMarshaller() {
      this(new ClassAllowList(Collections.emptyList()));
   }

   public JavaSerializationMarshaller(ClassAllowList allowList) {
      this.allowList = allowList;
   }

   @Override
   public void initialize(ClassAllowList classAllowList) {
      this.allowList.read(classAllowList);
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutput out = new ObjectOutputStream(baos);
      out.writeObject(o);
      out.close();
      baos.close();
      return ByteBufferImpl.create(baos.toByteArray());
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      try (ObjectInputStream ois = new CheckedInputStream(new ByteArrayInputStream(buf), allowList)) {
         return ois.readObject();
      }
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o instanceof Serializable;
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_SERIALIZED_OBJECT;
   }

}
