package org.jboss.as.clustering.infinispan.cs.deployment;

import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactory;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactoryService;
import org.jboss.as.server.deployment.*;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleClassLoader;
import org.jboss.msc.service.*;
import org.jboss.msc.value.InjectedValue;

import java.lang.reflect.Constructor;
import java.util.List;

public abstract class AbstractCacheStoreExtensionProcessor<T> implements DeploymentUnitProcessor {

   @Override
   public void deploy(DeploymentPhaseContext ctx) throws DeploymentUnitProcessingException {
      DeploymentUnit deploymentUnit = ctx.getDeploymentUnit();
      Module module = deploymentUnit.getAttachment(Attachments.MODULE);
      ServicesAttachment servicesAttachment = deploymentUnit.getAttachment(Attachments.SERVICES);
      if (module != null && servicesAttachment != null)
         addServices(ctx, servicesAttachment, module);
   }

   @Override
   public void undeploy(DeploymentUnit deploymentUnit) {
      // Deploy only adds services, so no need to do anything here
      // since these services are automatically removed.
   }

   private void addServices(DeploymentPhaseContext ctx, ServicesAttachment servicesAttachment, Module module) {
      Class<T> serviceClass = getServiceClass();
      List<String> implementationNames = servicesAttachment.getServiceImplementations(serviceClass.getName());
      ModuleClassLoader classLoader = module.getClassLoader();
      for (String serviceClassName : implementationNames) {
         try {
            Class<? extends T> clazz = classLoader.loadClass(serviceClassName).asSubclass(serviceClass);
            Constructor<? extends T> ctor = clazz.getConstructor();
            T instance = ctor.newInstance();
            installService(ctx, serviceClassName, instance);
         } catch (Exception e) {
            InfinispanMessages.MESSAGES.unableToInstantiateClass(serviceClassName);
         }
      }
   }

   public final void installService(DeploymentPhaseContext ctx, String implementationClassName, T instance) {
      AbstractExtensionManagerService<T> service = createService(implementationClassName, instance);
      ServiceName extensionServiceName = ServiceName.JBOSS.append(service.getServiceTypeName(), implementationClassName.replaceAll("\\.", "_"));
      InfinispanLogger.ROOT_LOGGER.installDeployedCacheStore(implementationClassName);
      ServiceBuilder<T> serviceBuilder = ctx.getServiceTarget().addService(extensionServiceName, service);
      serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
      serviceBuilder.addDependency(DeployedCacheStoreFactoryService.SERVICE_NAME, DeployedCacheStoreFactory.class, service.getDeployedCacheStoreFactory());
      serviceBuilder.install();
   }

   public abstract Class<T> getServiceClass();

   public abstract AbstractExtensionManagerService<T> createService(String serviceName, T instance);

   protected static abstract class AbstractExtensionManagerService<T> implements Service<T> {

      protected final T extension;
      protected final String className;
      protected InjectedValue<DeployedCacheStoreFactory> deployedCacheStoreFactory = new InjectedValue<>();

      protected AbstractExtensionManagerService(String className, T extension) {
         this.extension = extension;
         this.className = className;
      }

      @Override
      public void start(StartContext context) {
         InfinispanLogger.ROOT_LOGGER.deployedStoreStarted(className);
         deployedCacheStoreFactory.getValue().addInstance(extension);
      }

      @Override
      public void stop(StopContext context) {
         InfinispanLogger.ROOT_LOGGER.deployedStoreStopped(className);
         deployedCacheStoreFactory.getValue().removeInstance(extension);
      }

      public InjectedValue<DeployedCacheStoreFactory> getDeployedCacheStoreFactory() {
         return deployedCacheStoreFactory;
      }

      public abstract String getServiceTypeName();
   }
}
