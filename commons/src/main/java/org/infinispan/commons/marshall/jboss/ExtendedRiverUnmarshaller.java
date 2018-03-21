package org.infinispan.commons.marshall.jboss;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.reflect.SerializableClassRegistry;
import org.jboss.marshalling.river.RiverMarshallerFactory;
import org.jboss.marshalling.river.RiverUnmarshaller;

/**
 * An extended {@link RiverUnmarshaller} that allows Infinispan {@link StreamingMarshaller}
 * instances to travel down the stack to potential externalizer implementations
 * that might need it, such as {@link org.infinispan.commons.marshall.MarshalledValue.Externalizer}
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class ExtendedRiverUnmarshaller extends RiverUnmarshaller {

   private StreamingMarshaller infinispanMarshaller;
   private RiverCloseListener listener;

   protected ExtendedRiverUnmarshaller(RiverMarshallerFactory factory,
         SerializableClassRegistry registry, MarshallingConfiguration cfg) {
      super(factory, registry, cfg);
   }

   public StreamingMarshaller getInfinispanMarshaller() {
      return infinispanMarshaller;
   }

   public void setInfinispanMarshaller(StreamingMarshaller infinispanMarshaller) {
      this.infinispanMarshaller = infinispanMarshaller;
   }

   void setCloseListener(RiverCloseListener closeListener) {
      this.listener = closeListener;
   }

   @Override
   public void finish() throws IOException {
      super.finish();
      if (listener != null) {
         listener.closeUnmarshaller();
      }
   }

   void trimInstanceCache() {
      ArrayList instanceCache = (ArrayList) ReflectionUtil
         .getValue(this, "instanceCache");

      instanceCache.trimToSize();
   }

}
