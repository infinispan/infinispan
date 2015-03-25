package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.persistence.spi.AdvancedCacheLoader;

public final class AdvancedCacheLoaderExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedCacheLoader> {

   @Override
   public AdvancedCacheLoaderService createService(String serviceName, AdvancedCacheLoader instance) {
      return new AdvancedCacheLoaderService(serviceName, instance);
   }

   @Override
   public Class<AdvancedCacheLoader> getServiceClass() {
      return AdvancedCacheLoader.class;
   }

   private static class AdvancedCacheLoaderService extends AbstractExtensionManagerService<AdvancedCacheLoader> {
      private AdvancedCacheLoaderService(String serviceName, AdvancedCacheLoader AdvancedCacheLoader) {
         super(serviceName, AdvancedCacheLoader);
      }

      @Override
      public AdvancedCacheLoader getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "AdvancedCacheLoader-service";
      }
   }

}
