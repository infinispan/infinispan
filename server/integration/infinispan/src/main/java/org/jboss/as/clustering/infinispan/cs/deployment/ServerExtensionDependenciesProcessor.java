package org.jboss.as.clustering.infinispan.cs.deployment;

import org.infinispan.Version;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.ExternalStore;
import org.infinispan.tasks.ServerTask;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.ServicesAttachment;
import org.jboss.as.server.deployment.module.ModuleDependency;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoader;

/**
 * Adds dependency from deployed module to Infinispan core.
 *
 * @author Sebastian Laskawiec
 * @since 7.2
 */
public class ServerExtensionDependenciesProcessor implements DeploymentUnitProcessor {

   private static final ModuleIdentifier API = ModuleIdentifier.create("org.infinispan", Version.getModuleSlot());
   private static final ModuleIdentifier TASKS_API = ModuleIdentifier.create("org.infinispan.tasks.api", Version.getModuleSlot());

   @Override
   public void deploy(DeploymentPhaseContext ctx) {
      DeploymentUnit deploymentUnit = ctx.getDeploymentUnit();
      if (hasServerTaskExtensions(deploymentUnit)) {
         addDependencies(deploymentUnit, API, TASKS_API);
      } else if (hasInfinispanExtensions(deploymentUnit)) {
         addDependencies(deploymentUnit, API);
      }
   }

   private void addDependencies(DeploymentUnit deploymentUnit, ModuleIdentifier... identifiers) {
      ModuleSpecification moduleSpec = deploymentUnit.getAttachment(Attachments.MODULE_SPECIFICATION);
      ModuleLoader moduleLoader = Module.getBootModuleLoader();
      for (ModuleIdentifier identifier : identifiers) {
         moduleSpec.addSystemDependency(new ModuleDependency(moduleLoader, identifier, false, false, false, false));
      }
   }

   private boolean hasInfinispanExtensions(DeploymentUnit deploymentUnit) {
      ServicesAttachment sa = deploymentUnit.getAttachment(Attachments.SERVICES);
      return sa != null && hasDeployableCache(sa);
   }

   private boolean hasServerTaskExtensions(DeploymentUnit deploymentUnit) {
      ServicesAttachment sa = deploymentUnit.getAttachment(Attachments.SERVICES);
      return sa != null && hasServerTasks(sa);
   }

   @Override
   public void undeploy(DeploymentUnit context) {
      // No-op
   }


   private boolean hasDeployableCache(ServicesAttachment sa) {
      return hasAdvancedCacheLoaders(sa) || hasAdvancedCacheWriters(sa) || hasAdvancedLoadWriteStores(sa) ||
            hasCacheLoader(sa) || hasCacheWriter(sa) || hasExternalStores(sa) || hasCustomMergePolicy(sa);
   }

   private boolean hasServerTasks(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, ServerTask.class);
   }

   private boolean hasAdvancedCacheLoaders(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, AdvancedCacheLoader.class);
   }

   private boolean hasAdvancedCacheWriters(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, AdvancedCacheWriter.class);
   }

   private boolean hasAdvancedLoadWriteStores(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, AdvancedLoadWriteStore.class);
   }

   private boolean hasCacheLoader(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, CacheLoader.class);
   }

   private boolean hasCacheWriter(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, CacheWriter.class);
   }

   private boolean hasExternalStores(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, ExternalStore.class);
   }

   private boolean hasCustomMergePolicy(ServicesAttachment servicesAttachment) {
      return hasExtension(servicesAttachment, EntryMergePolicy.class);
   }

   private boolean hasExtension(ServicesAttachment servicesAttachment, Class<?> extensionClass) {
      return !servicesAttachment.getServiceImplementations(extensionClass.getName()).isEmpty();
   }

}
