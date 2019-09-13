package org.jboss.as.clustering.infinispan.cs.deployment;

import java.util.function.Supplier;

import org.infinispan.persistence.spi.AdvancedCacheWriter;

public final class AdvancedCacheWriterExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedCacheWriter> {

   @Override
   public AdvancedCacheWriterService createService(String implClassName, Supplier<AdvancedCacheWriter> instanceFactory) {
      return new AdvancedCacheWriterService(implClassName, instanceFactory);
   }

   @Override
   public Class<AdvancedCacheWriter> getServiceClass() {
      return AdvancedCacheWriter.class;
   }

   private static class AdvancedCacheWriterService extends AbstractExtensionManagerService<AdvancedCacheWriter> {
      private AdvancedCacheWriterService(String className, Supplier<AdvancedCacheWriter> instanceFactory) {
         super("AdvancedCacheWriter-service", className, instanceFactory);
      }
   }
}
