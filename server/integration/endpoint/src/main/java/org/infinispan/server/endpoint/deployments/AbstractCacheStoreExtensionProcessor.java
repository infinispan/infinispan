package org.infinispan.server.endpoint.deployments;

import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.endpoint.subsystem.ExtensionManagerService;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.value.InjectedValue;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public abstract class AbstractCacheStoreExtensionProcessor<T> extends AbstractServerExtensionProcessor<T> {

   private final ServiceName extensionManagerServiceName;

   protected AbstractCacheStoreExtensionProcessor(ServiceName extensionManagerServiceName) {
      this.extensionManagerServiceName = extensionManagerServiceName;
   }

   @Override
   public final void installService(DeploymentPhaseContext ctx, String serviceName, T instance) {
      AbstractExtensionManagerService<T> service = createService(serviceName, instance);
      ServiceName extensionServiceName = Constants.DATAGRID.append(service.getServiceTypeName(), serviceName.replaceAll("\\.", "_"));
      ServiceBuilder<T> serviceBuilder = ctx.getServiceTarget().addService(extensionServiceName, service);
      serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE)
            .addDependency(extensionManagerServiceName, ExtensionManagerService.class, service.getExtensionManager());
      serviceBuilder.install();
   }

   public abstract AbstractExtensionManagerService<T> createService(String serviceName, T instance);

   protected static abstract class AbstractExtensionManagerService<T> implements Service<T> {

      protected final T extension;
      protected final String serviceName;
      protected final InjectedValue<ExtensionManagerService> extensionManager = new InjectedValue<>();

      protected AbstractExtensionManagerService(String serviceName, T extension) {
         assert extension != null : ROOT_LOGGER.nullVar(getServiceTypeName());
         this.extension = extension;
         this.serviceName = serviceName;
      }

      public InjectedValue<ExtensionManagerService> getExtensionManager() {
         return extensionManager;
      }

      public abstract String getServiceTypeName();
   }
}
