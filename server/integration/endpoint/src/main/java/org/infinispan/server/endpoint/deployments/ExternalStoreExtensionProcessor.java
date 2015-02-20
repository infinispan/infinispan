package org.infinispan.server.endpoint.deployments;

import org.infinispan.persistence.spi.ExternalStore;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class ExternalStoreExtensionProcessor extends AbstractCacheStoreExtensionProcessor<ExternalStore> {

   public ExternalStoreExtensionProcessor(ServiceName extensionManagerServiceName) {
      super(extensionManagerServiceName);
   }

   @Override
   public AbstractExtensionManagerService<ExternalStore> createService(String serviceName, ExternalStore instance) {
      return new ExternalStoreService(serviceName, instance);
   }

   @Override
   public Class<ExternalStore> getServiceClass() {
      return ExternalStore.class;
   }

   private static class ExternalStoreService extends AbstractExtensionManagerService<ExternalStore> {
      private ExternalStoreService(String serviceName, ExternalStore ExternalStore) {
         super(serviceName, ExternalStore);
      }

      @Override
      public void start(StartContext context) {
         ROOT_LOGGER.debugf("Started ExternalStore service with name = %s", serviceName);
         extensionManager.getValue().addCacheStore(serviceName, extension);
      }

      @Override
      public void stop(StopContext context) {
         ROOT_LOGGER.debugf("Stopped ExternalStore service with name = %s", serviceName);
         extensionManager.getValue().removeCacheStore(serviceName);
      }

      @Override
      public ExternalStore getValue() {
         return extension;
      }

      @Override
      public String getServiceTypeName() {
         return "ExternalStore-service";
      }
   }

}
