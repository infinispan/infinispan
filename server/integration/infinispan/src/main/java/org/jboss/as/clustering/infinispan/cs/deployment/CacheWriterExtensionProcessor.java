package org.jboss.as.clustering.infinispan.cs.deployment;

import java.util.function.Supplier;

import org.infinispan.persistence.spi.CacheWriter;

public final class CacheWriterExtensionProcessor extends AbstractCacheStoreExtensionProcessor<CacheWriter> {

   @Override
   public CacheWriterService createService(String implClassName, Supplier<CacheWriter> instanceFactory) {
      return new CacheWriterService(implClassName, instanceFactory);
   }

   @Override
   public Class<CacheWriter> getServiceClass() {
      return CacheWriter.class;
   }

   private static class CacheWriterService extends AbstractExtensionManagerService<CacheWriter> {
      private CacheWriterService(String className, Supplier<CacheWriter> instanceFactory) {
         super("CacheWriter-service", className, instanceFactory);
      }
   }
}
