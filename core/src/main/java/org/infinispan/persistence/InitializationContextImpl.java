package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.util.TimeService;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class InitializationContextImpl implements InitializationContext {

   private final StoreConfiguration configuration;
   private final Cache cache;
   private final StreamingMarshaller marshaller;
   private final TimeService timeService;

   public InitializationContextImpl(StoreConfiguration configuration, Cache cache, StreamingMarshaller marshaller, TimeService timeService) {
      this.configuration = configuration;
      this.cache = cache;
      this.marshaller = marshaller;
      this.timeService = timeService;
   }

   @Override
   public StoreConfiguration getConfiguration() {
      return configuration;
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
      return timeService;
   }
}
