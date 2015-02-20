package org.infinispan.server.endpoint.deployments;

import org.infinispan.persistence.spi.CacheLoader;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class CacheLoaderExtensionProcessor extends AbstractCacheStoreExtensionProcessor<CacheLoader> {

   public CacheLoaderExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<CacheLoader> createService(String serviceName, CacheLoader instance) {
      return new CacheLoaderService(serviceName, instance);
   }

   @Override
   public Class<CacheLoader> getServiceClass() {
      return CacheLoader.class;
   }

   private static class CacheLoaderService extends AbstractExtensionManagerService<CacheLoader> {
      private CacheLoaderService(String serviceName, CacheLoader cacheLoader) {
         super(serviceName, cacheLoader);
      }

      @Override
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started CacheLoader service with name = %s", serviceName);
         extensionManager.getValue().addCacheStore(serviceName, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped CacheLoader service with name = %s", serviceName);
         extensionManager.getValue().removeCacheStore(serviceName);
      }

      @Override
      public CacheLoader getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "CacheLoader-service";
      }
   }

}
