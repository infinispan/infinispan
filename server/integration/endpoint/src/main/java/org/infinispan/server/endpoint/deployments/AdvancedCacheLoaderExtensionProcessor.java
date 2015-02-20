package org.infinispan.server.endpoint.deployments;

import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class AdvancedCacheLoaderExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedCacheLoader> {

   public AdvancedCacheLoaderExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<AdvancedCacheLoader> createService(String serviceName, AdvancedCacheLoader instance) {
      return new AdvancedCacheLoaderService(serviceName, instance);
   }

   @Override
   public Class<AdvancedCacheLoader> getServiceClass() {
      return AdvancedCacheLoader.class;
   }

   private static class AdvancedCacheLoaderService extends AbstractExtensionManagerService<AdvancedCacheLoader> {
      private AdvancedCacheLoaderService(String serviceName, AdvancedCacheLoader AdvancedCacheLoader) {
         super(serviceName, AdvancedCacheLoader);
      }

      @Override
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started AdvancedCacheLoader service with name = %s", serviceName);
         extensionManager.getValue().addCacheStore(serviceName, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped AdvancedCacheLoader service with name = %s", serviceName);
         extensionManager.getValue().removeCacheStore(serviceName);
      }

      @Override
      public AdvancedCacheLoader getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "AdvancedCacheLoader-service";
      }
   }

}
