package org.infinispan.server.endpoint.deployments;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.ExternalStore;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.ServicesAttachment;
import org.jboss.as.server.deployment.module.ModuleDependency;
import org.jboss.as.server.deployment.module.ModuleSpecification;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleIdentifier;
import org.jboss.modules.ModuleLoader;

public class ServerExtensionDependenciesProcessor implements DeploymentUnitProcessor {

    private static final ModuleIdentifier API = ModuleIdentifier.create("org.infinispan");

    @Override
    public void deploy(DeploymentPhaseContext ctx) throws DeploymentUnitProcessingException {
        if (hasInfinispanExtensions(ctx)) {
            DeploymentUnit deploymentUnit = ctx.getDeploymentUnit();
            ModuleSpecification moduleSpec = deploymentUnit.getAttachment(Attachments.MODULE_SPECIFICATION);
            ModuleLoader moduleLoader = Module.getBootModuleLoader();
            moduleSpec.addSystemDependency(new ModuleDependency(moduleLoader, API, false, false, false, false));
        }
    }

    private boolean hasInfinispanExtensions(DeploymentPhaseContext ctx) {
        DeploymentUnit deploymentUnit = ctx.getDeploymentUnit();
        ServicesAttachment sa = deploymentUnit.getAttachment(Attachments.SERVICES);
        if (sa != null) {
            return hasFilterFactories(sa) || hasMarshallers(sa) || hasConverterFactories(sa) || hasDeployableCache(sa);
        }
        return false;
    }

    @Override
    public void undeploy(DeploymentUnit context) {
        // No-op
    }

   private boolean hasFilterFactories(ServicesAttachment servicesAttachment) {
       return hasExtension(servicesAttachment, CacheEventFilterFactory.class);
   }

   private boolean hasMarshallers(ServicesAttachment servicesAttachment) {
       return hasExtension(servicesAttachment, Marshaller.class);
   }

   private boolean hasConverterFactories(ServicesAttachment servicesAttachment) {
       return hasExtension(servicesAttachment, CacheEventConverterFactory.class);
   }

   private boolean hasDeployableCache(ServicesAttachment sa) {
       return hasAdvancedCacheLoaders(sa) || hasAdvancedCacheWriters(sa) || hasAdvancedLoadWriteStores(sa) ||
             hasCacheLoader(sa) || hasCacheWriter(sa) || hasExternalStores(sa);
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

   private boolean hasExtension(ServicesAttachment servicesAttachment, Class<?> extensionClass) {
       return !servicesAttachment.getServiceImplementations(extensionClass.getName()).isEmpty();
   }

}
