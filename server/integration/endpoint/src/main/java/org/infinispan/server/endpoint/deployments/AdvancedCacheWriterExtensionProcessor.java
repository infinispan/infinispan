package org.infinispan.server.endpoint.deployments;

import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class AdvancedCacheWriterExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedCacheWriter> {

   public AdvancedCacheWriterExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<AdvancedCacheWriter> createService(String serviceName, AdvancedCacheWriter instance) {
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
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started AdvancedCacheWriter service with name = %s", serviceName);
         extensionManager.getValue().addCacheStore(serviceName, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped AdvancedCacheWriter service with name = %s", serviceName);
         extensionManager.getValue().removeCacheStore(serviceName);
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
