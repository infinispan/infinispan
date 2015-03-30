package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.persistence.spi.ExternalStore;

public final class ExternalStoreExtensionProcessor extends AbstractCacheStoreExtensionProcessor<ExternalStore> {

   @Override
   public ExternalStoreService createService(String serviceName, ExternalStore instance) {
      return new ExternalStoreService(serviceName, instance);
   }

   @Override
   public Class<ExternalStore> getServiceClass() {
      return ExternalStore.class;
   }

   private static class ExternalStoreService extends AbstractExtensionManagerService<ExternalStore> {
      private ExternalStoreService(String serviceName, ExternalStore ExternalStore) {
         super(serviceName, ExternalStore);
      }

      @Override
      public ExternalStore getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "ExternalStore-service";
      }
   }

}
