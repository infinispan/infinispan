package org.jboss.as.clustering.infinispan.cs.deployment;

import java.util.function.Supplier;

import org.infinispan.persistence.spi.CacheLoader;

public final class CacheLoaderExtensionProcessor extends AbstractCacheStoreExtensionProcessor<CacheLoader> {

   @Override
   public CacheLoaderService createService(String implClassName, Supplier<CacheLoader> instanceFactory) {
      return new CacheLoaderService(implClassName, instanceFactory);
   }

   @Override
   public Class<CacheLoader> getServiceClass() {
      return CacheLoader.class;
   }

   private static class CacheLoaderService extends AbstractExtensionManagerService<CacheLoader> {
      private CacheLoaderService(String className, Supplier<CacheLoader> instanceFactory) {
         super("CacheLoader-service", className, instanceFactory);
      }
   }
}
