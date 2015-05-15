package org.infinispan.server.endpoint.deployments;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
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

import java.util.List;

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
        ServicesAttachment servicesAttachment = deploymentUnit.getAttachment(Attachments.SERVICES);
        if (servicesAttachment != null) {
            List<String> filterFactories = servicesAttachment.getServiceImplementations(CacheEventFilterFactory.class.getName());
            List<String> converterFactories = servicesAttachment.getServiceImplementations(CacheEventConverterFactory.class.getName());
            List<String> filterConverterFactories = servicesAttachment.getServiceImplementations(CacheEventFilterConverterFactory.class.getName());
            List<String> marshallers = servicesAttachment.getServiceImplementations(Marshaller.class.getName());
            List<String> keyValueFilterConverterFactories = servicesAttachment.getServiceImplementations(KeyValueFilterConverterFactory.class.getName());
            return !filterFactories.isEmpty() || !marshallers.isEmpty()
                    || !converterFactories.isEmpty() || !filterConverterFactories.isEmpty() || !keyValueFilterConverterFactories.isEmpty();
        }
        return false;
    }

    @Override
    public void undeploy(DeploymentUnit context) {
        // No-op
    }

}
