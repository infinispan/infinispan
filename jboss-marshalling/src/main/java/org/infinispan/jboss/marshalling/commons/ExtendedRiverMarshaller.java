package org.infinispan.jboss.marshalling.commons;

import java.io.IOException;

import org.jboss.marshalling.MarshallingConfiguration;
import org.jboss.marshalling.reflect.SerializableClassRegistry;
import org.jboss.marshalling.river.RiverMarshaller;
import org.jboss.marshalling.river.RiverMarshallerFactory;

/**
 * {@link RiverMarshaller} extension that allows Infinispan code to directly
 * create instances of it.
 *
 * @author Galder Zamarreño
 * @since 5.1
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11947.
 */
@Deprecated
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

}
