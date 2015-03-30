package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.persistence.spi.CacheLoader;

public final class CacheLoaderExtensionProcessor extends AbstractCacheStoreExtensionProcessor<CacheLoader> {

   @Override
   public CacheLoaderService createService(String serviceName, CacheLoader instance) {
      return new CacheLoaderService(serviceName, instance);
   }

   @Override
   public Class<CacheLoader> getServiceClass() {
      return CacheLoader.class;
   }

   private static class CacheLoaderService extends AbstractExtensionManagerService<CacheLoader> {
      private CacheLoaderService(String serviceName, CacheLoader cacheLoader) {
         super(serviceName, cacheLoader);
      }

      @Override
      public CacheLoader getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "CacheLoader-service";
      }
   }

}
