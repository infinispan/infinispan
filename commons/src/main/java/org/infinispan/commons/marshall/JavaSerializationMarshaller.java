package org.infinispan.commons.marshall;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;

import org.infinispan.commons.configuration.ClassWhiteList;
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

   final ClassWhiteList whiteList;

   public JavaSerializationMarshaller() {
      this(new ClassWhiteList(Collections.emptyList()));
   }

   public JavaSerializationMarshaller(ClassWhiteList whiteList) {
      this.whiteList = whiteList;
   }

   @Override
   public void initialize(ClassWhiteList classWhiteList) {
      this.whiteList.read(classWhiteList);
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutput out = new ObjectOutputStream(baos);
      out.writeObject(o);
      out.close();
      baos.close();
      byte[] bytes = baos.toByteArray();
      return new ByteBufferImpl(bytes, 0, bytes.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      try (ObjectInputStream ois = new CheckedInputStream(new ByteArrayInputStream(buf), whiteList)) {
         return ois.readObject();
      }
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return o instanceof Serializable;
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_SERIALIZED_OBJECT;
   }

}
