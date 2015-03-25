package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.persistence.spi.AdvancedLoadWriteStore;

public final class AdvancedLoadWriteStoreExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedLoadWriteStore> {

   @Override
   public AdvancedCacheLoaderService createService(String serviceName, AdvancedLoadWriteStore instance) {
      return new AdvancedCacheLoaderService(serviceName, instance);
   }

   @Override
   public Class<AdvancedLoadWriteStore> getServiceClass() {
      return AdvancedLoadWriteStore.class;
   }

   private static class AdvancedCacheLoaderService extends AbstractExtensionManagerService<AdvancedLoadWriteStore> {
      private AdvancedCacheLoaderService(String serviceName, AdvancedLoadWriteStore AdvancedLoadWriteStore) {
         super(serviceName, AdvancedLoadWriteStore);
      }

      @Override
      public AdvancedLoadWriteStore getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "AdvancedLoadWriteStore-service";
      }
   }

}
