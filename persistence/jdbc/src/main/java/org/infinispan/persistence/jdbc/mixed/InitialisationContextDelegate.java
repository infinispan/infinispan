package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.TimeService;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class InitialisationContextDelegate implements InitializationContext {

   private final InitializationContext actual;

   private final StoreConfiguration storeConfiguration;

   public InitialisationContextDelegate(InitializationContext actual, StoreConfiguration storeConfiguration) {
      this.actual = actual;
      this.storeConfiguration = storeConfiguration;
   }

   @Override
   public StoreConfiguration getConfiguration() {
      return storeConfiguration;
   }

   @Override
   public Cache getCache() {
      return actual.getCache();
   }

   @Override
   public StreamingMarshaller getMarshaller() {
      return actual.getMarshaller();
   }

   @Override
   public TimeService getTimeService() {
      return actual.getTimeService();
   }
}
