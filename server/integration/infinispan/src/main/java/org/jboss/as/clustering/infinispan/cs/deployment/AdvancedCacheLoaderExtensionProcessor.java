package org.jboss.as.clustering.infinispan.cs.deployment;

import java.util.function.Supplier;

import org.infinispan.persistence.spi.AdvancedCacheLoader;

public final class AdvancedCacheLoaderExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedCacheLoader> {

   @Override
   public AdvancedCacheLoaderService createService(String implClassName, Supplier<AdvancedCacheLoader> instanceFactory) {
      return new AdvancedCacheLoaderService(implClassName, instanceFactory);
   }

   @Override
   public Class<AdvancedCacheLoader> getServiceClass() {
      return AdvancedCacheLoader.class;
   }

   private static class AdvancedCacheLoaderService extends AbstractExtensionManagerService<AdvancedCacheLoader> {
      private AdvancedCacheLoaderService(String className, Supplier<AdvancedCacheLoader> instanceFactory) {
         super("AdvancedCacheLoader-service", className, instanceFactory);
      }
   }
}
