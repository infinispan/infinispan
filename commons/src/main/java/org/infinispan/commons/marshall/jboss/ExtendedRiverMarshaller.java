package org.infinispan.commons.marshall.jboss;

import org.infinispan.commons.util.ReflectionUtil;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.reflect.SerializableClassRegistry;
import org.jboss.marshalling.river.RiverMarshaller;
import org.jboss.marshalling.river.RiverMarshallerFactory;

import java.io.IOException;

/**
 * {@link RiverMarshaller} extension that allows Infinispan code to directly
 * create instances of it.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class ExtendedRiverMarshaller extends RiverMarshaller {

   private RiverCloseListener listener;

   public ExtendedRiverMarshaller(RiverMarshallerFactory factory,
         SerializableClassRegistry registry, MarshallingConfiguration cfg) throws IOException {
      super(factory, registry, cfg);
   }

   @Override
   public void finish() throws IOException {
      super.finish();
      if (listener != null) {
         listener.closeMarshaller();
      }
   }

   void setCloseListener(RiverCloseListener listener) {
      this.listener = listener;
   }

   int instanceCacheResizeCount() {
      Object instanceCache = ReflectionUtil
         .getValue(this, "instanceCache");

      final Object[] keys = (Object[]) ReflectionUtil
         .getValue(instanceCache, "keys");

      return keys.length;
   }

}
