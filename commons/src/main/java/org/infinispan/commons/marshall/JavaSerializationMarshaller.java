package org.infinispan.commons.marshall;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;

import java.io.*;

/**
 * Standard Java serialization marshaller.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JavaSerializationMarshaller extends AbstractMarshaller {

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
      InputStream bais = new ByteArrayInputStream(buf);
      ObjectInput in = new ObjectInputStream(bais);
      Object o = in.readObject();
      in.close();
      bais.close();
      return o;
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return o instanceof Serializable;
   }

}
