package org.infinispan.server.infinispan.task;

import org.infinispan.tasks.ServerTask;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.server.deployment.*;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleClassLoader;
import org.jboss.msc.service.*;
import org.jboss.msc.value.InjectedValue;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Author: Michal Szynkiewicz, michal.l.szynkiewicz@gmail.com
 * Date: 1/19/16
 * Time: 1:37 PM
 */
public class ServerTaskProcessor implements DeploymentUnitProcessor {

   public static final String EXTERNAL_TASK = "ExternalTask";

   @Override
   public void deploy(DeploymentPhaseContext phaseContext) throws DeploymentUnitProcessingException {
      DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
      Module module = deploymentUnit.getAttachment(Attachments.MODULE);
      ServicesAttachment servicesAttachment = deploymentUnit.getAttachment(Attachments.SERVICES);
      if (module != null && servicesAttachment != null)
         addServices(phaseContext, servicesAttachment, module);
   }

   private void addServices(DeploymentPhaseContext ctx, ServicesAttachment servicesAttachment, Module module) {
      List<String> implementationNames = servicesAttachment.getServiceImplementations(ServerTask.class.getName());
      ModuleClassLoader classLoader = module.getClassLoader();
      for (String serviceClassName : implementationNames) {
         try {
            Class<? extends ServerTask> clazz = classLoader.loadClass(serviceClassName).asSubclass(ServerTask.class);
            Constructor<? extends ServerTask> ctor = clazz.getConstructor();
            ServerTask instance = ctor.newInstance();
            installService(ctx, serviceClassName, instance);
         } catch (Exception e) {
            throw InfinispanMessages.MESSAGES.unableToInstantiateClass(serviceClassName);
         }
      }
   }

   public final void installService(DeploymentPhaseContext ctx, String implementationClassName, ServerTask instance) {
      TaskManagerService service = new TaskManagerService(implementationClassName, instance);
      ServiceName extensionServiceName = ServiceName.JBOSS.append(EXTERNAL_TASK, implementationClassName.replaceAll("\\.", "_"));
      InfinispanLogger.ROOT_LOGGER.installDeployedCacheStore(implementationClassName);
      ServiceBuilder<ServerTask> serviceBuilder = ctx.getServiceTarget().addService(extensionServiceName, service);
      serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE);
      serviceBuilder.addDependency(ServerTaskRegistryService.SERVICE_NAME, ServerTaskRegistry.class, service.getDeployedTaskManager());
      serviceBuilder.install();
   }

   @Override
   public void undeploy(DeploymentUnit context) {
   }

   protected static class TaskManagerService implements Service<ServerTask> {

      protected final ServerTask extension;
      protected final String className;
      protected InjectedValue<ServerTaskRegistry> deployedTaskRegistry = new InjectedValue<>();

      protected TaskManagerService(String className, ServerTask extension) {
         this.extension = extension;
         this.className = className;
      }

      @Override
      public void start(StartContext context) {
         InfinispanLogger.ROOT_LOGGER.deployedStoreStarted(className);
         deployedTaskRegistry.getValue().addDeployedTask(extension);
      }

      @Override
      public void stop(StopContext context) {
         InfinispanLogger.ROOT_LOGGER.deployedStoreStopped(className);
         deployedTaskRegistry.getValue().removeDeployedTask(extension.getName());
      }

      public InjectedValue<ServerTaskRegistry> getDeployedTaskManager() {
         return deployedTaskRegistry;
      }

      @Override
      public ServerTask getValue() {
         return extension;
      }
   }
}
