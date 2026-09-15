package org.infinispan.spring.common.marshalling;

import java.io.ByteArrayInputStream;
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
import org.infinispan.commons.io.LazyByteArrayOutputStream;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.commons.marshall.CheckedInputStream;

/**
 * Spring-owned Java serialization marshaller that decouples the Spring integration
 * from the core which will be deprecated {@code JavaSerializationMarshaller}.
 * <p>
 * The allow list is populated via {@link #initialize(ClassAllowList)} during
 * cache manager startup, matching the behavior of the core marshaller.
 */
public class SpringJavaSerializationMarshaller extends AbstractMarshaller {

   private final ClassAllowList allowList;

   public SpringJavaSerializationMarshaller() {
      this.allowList = new ClassAllowList(Collections.emptyList());
   }

   public SpringJavaSerializationMarshaller(ClassAllowList allowList) {
      this.allowList = allowList;
   }

   @Override
   public void initialize(ClassAllowList classAllowList) {
      this.allowList.read(classAllowList);
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException {
      LazyByteArrayOutputStream baos = new LazyByteArrayOutputStream();
      ObjectOutput out = new ObjectOutputStream(baos);
      out.writeObject(o);
      out.close();
      baos.close();
      return ByteBufferImpl.create(baos.getRawBuffer(), 0, baos.size());
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length)
         throws IOException, ClassNotFoundException {
      try (ObjectInputStream ois = new CheckedInputStream(
            new ByteArrayInputStream(buf, offset, length), allowList)) {
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
