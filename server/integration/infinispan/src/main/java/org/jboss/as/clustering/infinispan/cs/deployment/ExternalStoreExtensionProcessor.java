package org.jboss.as.clustering.infinispan.cs.deployment;

import java.util.function.Supplier;

import org.infinispan.persistence.spi.ExternalStore;

public final class ExternalStoreExtensionProcessor extends AbstractCacheStoreExtensionProcessor<ExternalStore> {

   @Override
   public ExternalStoreService createService(String implClassName, Supplier<ExternalStore> instanceFactory) {
      return new ExternalStoreService(implClassName, instanceFactory);
   }

   @Override
   public Class<ExternalStore> getServiceClass() {
      return ExternalStore.class;
   }

   private static class ExternalStoreService extends AbstractExtensionManagerService<ExternalStore> {
      private ExternalStoreService(String className, Supplier<ExternalStore> instanceFactory) {
         super("ExternalStore-service", className, instanceFactory);
      }
   }
}
