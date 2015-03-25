package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.persistence.spi.AdvancedCacheWriter;

public final class AdvancedCacheWriterExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedCacheWriter> {

   @Override
   public AdvancedCacheWriterService createService(String serviceName, AdvancedCacheWriter instance) {
      return new AdvancedCacheWriterService(serviceName, instance);
   }

   @Override
   public Class<AdvancedCacheWriter> getServiceClass() {
      return AdvancedCacheWriter.class;
   }

   private static class AdvancedCacheWriterService extends AbstractExtensionManagerService<AdvancedCacheWriter> {
      private AdvancedCacheWriterService(String serviceName, AdvancedCacheWriter AdvancedCacheWriter) {
         super(serviceName, AdvancedCacheWriter);
      }

      @Override
      public AdvancedCacheWriter getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "AdvancedCacheWriter-service";
      }
   }

}
