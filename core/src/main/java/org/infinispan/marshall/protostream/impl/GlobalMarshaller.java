package org.infinispan.marshall.protostream.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.io.IOException;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.protostream.ImmutableSerializationContext;

/**
 * A globally-scoped marshaller for cluster communication.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
// TODO add support for reusing instances similar to InstanceReusingAdvancedExternalizer?
public class GlobalMarshaller extends AbstractInternalProtoStreamMarshaller {

   @Inject
   GlobalComponentRegistry gcr;

   private ClassLoader classLoader;

   public GlobalMarshaller() {
      super(CONTAINER);
   }

   @Override
   public void start() {
      super.start();
      classLoader = gcr.getGlobalConfiguration().classLoader();
   }

   @Override
   @Stop() // Stop after transport to avoid send/receive and marshaller not being ready
   public void stop() {
      userMarshaller.stop();
   }

   @Override
   public ImmutableSerializationContext getSerializationContext() {
      return ctxRegistry.getGlobalCtx();
   }

   @Override
   protected boolean isMarshallableWithProtoStream(Object o) {
      return o instanceof String ||
            o instanceof Long ||
            o instanceof Integer ||
            o instanceof Double ||
            o instanceof Float ||
            o instanceof Boolean ||
            o instanceof byte[] ||
            o instanceof Byte ||
            o instanceof Short ||
            o instanceof Character ||
            o instanceof java.util.Date ||
            o instanceof java.time.Instant ||
            super.isMarshallableWithProtoStream(o);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) {
      if (obj == null)
         return null;

      Class<?> clazz = obj.getClass();
      if (clazz.isSynthetic()) {
         obj = MarshallableLambda.create(obj);
      } else if (obj instanceof Throwable && !isMarshallable(obj)) {
         obj = MarshallableThrowable.create((Throwable) obj);
      }

      return super.objectToByteBuffer(obj, estimatedSize);
   }

   @Override
   protected Object unwrapAndInit(Object o) {
      if (o instanceof MarshallableLambda) {
         return ((MarshallableLambda) o).unwrap(classLoader);
      } else if (o instanceof MarshallableThrowable) {
         return ((MarshallableThrowable) o).get();
      }

      return super.unwrapAndInit(o);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException {
      return super.objectFromByteBuffer(buf, offset, length);
   }
}
