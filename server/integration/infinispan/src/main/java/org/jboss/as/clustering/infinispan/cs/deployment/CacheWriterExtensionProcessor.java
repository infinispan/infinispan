package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.persistence.spi.CacheWriter;

public final class CacheWriterExtensionProcessor extends AbstractCacheStoreExtensionProcessor<CacheWriter> {

   @Override
   public CacheWriterService createService(String serviceName, CacheWriter instance) {
      return new CacheWriterService(serviceName, instance);
   }

   @Override
   public Class<CacheWriter> getServiceClass() {
      return CacheWriter.class;
   }

   private static class CacheWriterService extends AbstractExtensionManagerService<CacheWriter> {
      private CacheWriterService(String serviceName, CacheWriter CacheWriter) {
         super(serviceName, CacheWriter);
      }

      @Override
      public CacheWriter getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "CacheWriter-service";
      }
   }

}
