package org.jboss.as.clustering.infinispan.conflict;

import java.lang.reflect.Constructor;
import java.util.List;

import org.infinispan.conflict.EntryMergePolicy;
import org.jboss.as.clustering.infinispan.InfinispanMessages;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.ServicesAttachment;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleClassLoader;
import org.jboss.msc.service.ServiceController;

public class DeployedMergePolicyProcessor implements DeploymentUnitProcessor {

   @Override
   public void deploy(DeploymentPhaseContext ctx) throws DeploymentUnitProcessingException {
      DeploymentUnit deploymentUnit = ctx.getDeploymentUnit();
      Module module = deploymentUnit.getAttachment(Attachments.MODULE);
      ServicesAttachment servicesAttachment = deploymentUnit.getAttachment(Attachments.SERVICES);
      if (module != null && servicesAttachment != null) {
         List<String> implementationNames = servicesAttachment.getServiceImplementations(EntryMergePolicy.class.getName());
         ModuleClassLoader classLoader = module.getClassLoader();
         for (String serviceClassName : implementationNames) {
            try {
               ServiceController sc = ctx.getServiceRegistry().getRequiredService(DeployedMergePolicyFactoryService.SERVICE_NAME);
               DeployedMergePolicyFactory factory = DeployedMergePolicyFactory.class.cast(sc.getValue());

               Class<? extends EntryMergePolicy> clazz = classLoader.loadClass(serviceClassName).asSubclass(EntryMergePolicy.class);
               Constructor<? extends EntryMergePolicy> ctor = clazz.getConstructor();
               factory.addDeployedPolicy(new DeployedMergePolicy(ctor.newInstance()));
            } catch (Exception e) {
               throw InfinispanMessages.MESSAGES.unableToInstantiateClass(serviceClassName);
            }
         }
      }
   }

   @Override
   public void undeploy(DeploymentUnit context) {
   }
}
