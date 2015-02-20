package org.infinispan.server.endpoint.deployments;

import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class AdvancedLoadWriteStoreExtensionProcessor extends AbstractCacheStoreExtensionProcessor<AdvancedLoadWriteStore> {

   public AdvancedLoadWriteStoreExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<AdvancedLoadWriteStore> createService(String serviceName, AdvancedLoadWriteStore instance) {
      return new AdvancedCacheLoaderService(serviceName, instance);
   }

   @Override
   public Class<AdvancedLoadWriteStore> getServiceClass() {
      return AdvancedLoadWriteStore.class;
   }

   private static class AdvancedCacheLoaderService extends AbstractExtensionManagerService<AdvancedLoadWriteStore> {
      private AdvancedCacheLoaderService(String serviceName, AdvancedLoadWriteStore AdvancedLoadWriteStore) {
         super(serviceName, AdvancedLoadWriteStore);
      }

      @Override
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started AdvancedLoadWriteStore service with name = %s", serviceName);
         extensionManager.getValue().addCacheStore(serviceName, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped AdvancedLoadWriteStore service with name = %s", serviceName);
         extensionManager.getValue().removeCacheStore(serviceName);
      }

      @Override
      public AdvancedLoadWriteStore getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "AdvancedLoadWriteStore-service";
      }
   }

}
