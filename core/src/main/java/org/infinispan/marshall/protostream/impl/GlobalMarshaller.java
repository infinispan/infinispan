package org.infinispan.marshall.protostream.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ImmutableProtoStreamMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.marshall.core.impl.DelegatingUserMarshaller;
import org.infinispan.protostream.ImmutableSerializationContext;

/**
 * A globally-scoped marshaller for cluster communication.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
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
      skipUserMarshaller = ((DelegatingUserMarshaller) userMarshaller).getDelegate() instanceof ImmutableProtoStreamMarshaller;
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
   public boolean isMarshallableWithProtoStream(Object o) {
      return isMarshallableWithoutWrapping(o) || o.getClass().isSynthetic() || o instanceof Throwable;
   }

   public boolean isMarshallableWithoutWrapping(Object o) {
      if (isProtostreamNativeType(o))
         return true;

      if (!skipUserMarshaller && o instanceof Iterable<?> iterable) {
         // Custom user marshaller configured so we need to make sure that nested classes in objects are also marshallable
         // with ProtoStream
         // When https://github.com/infinispan/protostream/issues/588 has been fixed, it should be possible to remove
         // this code and remove the SetAdapter and ListAdapter from the GLOBAL SerializationContext in the
         // SerializationContextRegistry when a custom user marshaller is configured.
         var it = iterable.iterator();
         if (it.hasNext()) {
            return isMarshallableWithoutWrapping(it.next());
         }
      }
      return super.isMarshallableWithProtoStream(o);
   }

   private boolean isProtostreamNativeType(Object o) {
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
            o instanceof java.time.Instant;
   }

   @Override
   public ByteBuffer objectToBuffer(Object obj) {
      return super.objectToBuffer(wrap(obj));
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) {
      return super.objectToByteBuffer(wrap(obj), estimatedSize);
   }

   private Object wrap(Object obj) {
      if (obj == null)
         return null;

      Class<?> clazz = obj.getClass();
      if (clazz.isSynthetic()) {
         obj = MarshallableLambda.create(obj);
      } else if (obj instanceof Throwable && !isMarshallableWithoutWrapping(obj)) {
         obj = MarshallableThrowable.create((Throwable) obj);
      }
      return obj;
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
}
