package org.infinispan.persistence;

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
public class DummyLoaderContext implements InitializationContext {
   StoreConfiguration clc;
   Cache cache;
   StreamingMarshaller marshaller;

   public DummyLoaderContext() {
   }

   public DummyLoaderContext(StoreConfiguration clc, Cache cache, StreamingMarshaller marshaller) {
      this.clc = clc;
      this.cache = cache;
      this.marshaller = marshaller;
   }

   @Override
   public StoreConfiguration getConfiguration() {
      return clc;
   }

   @Override
   public Cache getCache() {
      return cache;
   }

   @Override
   public StreamingMarshaller getMarshaller() {
      return marshaller;
   }

   @Override
   public TimeService getTimeService() {
      return cache.getAdvancedCache().getComponentRegistry().getTimeService();
   }
}
