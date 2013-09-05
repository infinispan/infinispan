package org.infinispan.persistence.support;

import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheWriter;

import java.util.concurrent.Executor;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class AdvancedSingletonCacheWriter extends SingletonCacheWriter implements AdvancedCacheWriter {

   public AdvancedSingletonCacheWriter(CacheWriter actual, SingletonStoreConfiguration singletonConfiguration) {
      super(actual, singletonConfiguration);
   }

   @Override
   public void clear() {
      if (active) advancedWriter().clear();
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      if (active) advancedWriter().purge(threadPool, task);
   }

   private AdvancedCacheWriter advancedWriter() {
      return (AdvancedCacheWriter) actual;
   }
}
