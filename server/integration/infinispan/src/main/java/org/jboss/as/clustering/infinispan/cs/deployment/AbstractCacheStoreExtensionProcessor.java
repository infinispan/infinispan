package org.jboss.as.clustering.infinispan.cs.deployment;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.function.Supplier;

import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactory;
import org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactoryService;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.ServicesAttachment;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleClassLoader;
import org.jboss.msc.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

public abstract class AbstractCacheStoreExtensionProcessor<T> implements DeploymentUnitProcessor {

   @Override
   public void deploy(DeploymentPhaseContext ctx) {
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
            installService(ctx, serviceClassName, () -> {
               try {
                  return ctor.newInstance();
               } catch (Exception e) {
                  throw InfinispanMessages.MESSAGES.unableToInstantiateClass(serviceClassName);
               }
            });
         } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw InfinispanMessages.MESSAGES.unableToInstantiateClass(serviceClassName);
         }
      }
   }

   private void installService(DeploymentPhaseContext ctx, String implementationClassName, Supplier<T> instanceFactory) {
      AbstractExtensionManagerService<T> service = createService(implementationClassName, instanceFactory);
      InfinispanLogger.ROOT_LOGGER.installDeployedCacheStore(implementationClassName);

      ServiceBuilder<?> serviceBuilder = ctx.getServiceTarget().addService(service.getName()).setInitialMode(ServiceController.Mode.ACTIVE);
      service.deployedCacheStoreFactory = serviceBuilder.requires(DeployedCacheStoreFactoryService.SERVICE_NAME);
      serviceBuilder.setInstance(service);
      serviceBuilder.install();
   }

   public abstract Class<T> getServiceClass();

   public abstract AbstractExtensionManagerService<T> createService(String implClassName, Supplier<T> instanceFactory);

   static abstract class AbstractExtensionManagerService<T> implements Service {

      final Supplier<T> instanceFactory;
      final String className;
      final String serviceName;
      Supplier<DeployedCacheStoreFactory> deployedCacheStoreFactory;

      AbstractExtensionManagerService(String serviceName, String className, Supplier<T> instanceFactory) {
         this.serviceName = serviceName;
         this.className = className;
         this.instanceFactory = instanceFactory;
      }

      @Override
      public void start(StartContext context) {
         InfinispanLogger.ROOT_LOGGER.deployedStoreStarted(className);
         deployedCacheStoreFactory.get().addInstanceFactory(className, instanceFactory);
      }

      @Override
      public void stop(StopContext context) {
         InfinispanLogger.ROOT_LOGGER.deployedStoreStopped(className);
         deployedCacheStoreFactory.get().removeInstance(className);
      }

      public ServiceName getName() {
         return ServiceName.JBOSS.append(serviceName, className.replace(".", "_"));
      }
   }
}
