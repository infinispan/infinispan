package org.infinispan.server.endpoint.deployments;

import org.infinispan.persistence.spi.CacheWriter;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class CacheWriterExtensionProcessor extends AbstractCacheStoreExtensionProcessor<CacheWriter> {

   public CacheWriterExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<CacheWriter> createService(String serviceName, CacheWriter instance) {
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
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started CacheWriter service with name = %s", serviceName);
         extensionManager.getValue().addCacheStore(serviceName, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped CacheWriter service with name = %s", serviceName);
         extensionManager.getValue().removeCacheStore(serviceName);
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
