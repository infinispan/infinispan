package org.jboss.as.clustering.infinispan.cs.deployment;

import java.util.function.Supplier;

import org.infinispan.persistence.spi.AdvancedLoadWriteStore;

public final class AdvancedLoadWriteStoreExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedLoadWriteStore> {

   @Override
   public AdvancedCacheLoaderService createService(String implClassName, Supplier<AdvancedLoadWriteStore> instanceFactory) {
      return new AdvancedCacheLoaderService(implClassName, instanceFactory);
   }

   @Override
   public Class<AdvancedLoadWriteStore> getServiceClass() {
      return AdvancedLoadWriteStore.class;
   }

   private static class AdvancedCacheLoaderService extends AbstractExtensionManagerService<AdvancedLoadWriteStore> {
      private AdvancedCacheLoaderService(String className, Supplier<AdvancedLoadWriteStore> instanceFactory) {
         super("AdvancedLoadWriteStore-service", className, instanceFactory);
      }
   }

}
