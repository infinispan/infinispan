package org.infinispan.commons.marshall.proto;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * An extension of {@link ProtoStreamMarshaller} that is able to handle {@link RuntimeMarshallableWrapper}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class ProtoStreamUserMarshaller extends ProtoStreamMarshaller {

   private static final Log log = LogFactory.getLog(ProtoStreamMarshaller.class);

   public ProtoStreamUserMarshaller(SerializationContext serializationContext) {
      super(serializationContext);
      SerializationContextInitializer initializer = new UserSerializationContextInternalizerImpl();
      try {
         initializer.registerSchema(serializationContext);
         initializer.registerMarshallers(serializationContext);
      } catch (IOException e) {
         throw new CacheException("Exception encountered when initialising SerializationContext", e);
      }
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
      if (o instanceof RuntimeMarshallableWrapper) {
         try {
            ((RuntimeMarshallableWrapper) o).marshall(this);
         } catch (IOException | InterruptedException e) {
            throw log.unableToMarshallRuntimeObject(o.getClass().getSimpleName(), RuntimeMarshallableWrapper.class.getSimpleName());
         }
      }
      return super.objectToBuffer(o, estimatedSize);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      Object o = super.objectFromByteBuffer(buf, offset, length);
      if (o instanceof RuntimeMarshallableWrapper)
         ((RuntimeMarshallableWrapper) o).unmarshall(this);
      return o;
   }

   @Override
   public boolean isMarshallable(Object o) {
      if (o instanceof RuntimeMarshallableWrapper)
         o = ((RuntimeMarshallableWrapper) o).get();
      return super.isMarshallable(o);
   }
}
