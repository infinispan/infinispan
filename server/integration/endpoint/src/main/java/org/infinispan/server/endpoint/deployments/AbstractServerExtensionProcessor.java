package org.infinispan.server.endpoint.deployments;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.as.server.deployment.ServicesAttachment;
import org.jboss.modules.Module;
import org.jboss.modules.ModuleClassLoader;

import java.lang.reflect.Constructor;
import java.util.List;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public abstract class AbstractServerExtensionProcessor<T> implements DeploymentUnitProcessor {

    @Override
    public void deploy(DeploymentPhaseContext ctx) throws DeploymentUnitProcessingException {
        DeploymentUnit deploymentUnit = ctx.getDeploymentUnit();
        Module module = deploymentUnit.getAttachment(Attachments.MODULE);
        ServicesAttachment servicesAttachment = deploymentUnit.getAttachment(Attachments.SERVICES);
        if (module != null && servicesAttachment != null)
            addServices(ctx, servicesAttachment, module);
    }

    private void addServices(DeploymentPhaseContext ctx, ServicesAttachment servicesAttachment, Module module) {
        Class<T> serviceClass = getServiceClass();
        List<String> services = servicesAttachment.getServiceImplementations(serviceClass.getName());
        ModuleClassLoader classLoader = module.getClassLoader();
        for (String serviceName : services) {
            try {
                Class<? extends T> clazz = classLoader.loadClass(serviceName).asSubclass(serviceClass);
                Constructor<? extends T> ctor = clazz.getConstructor();
                T instance = ctor.newInstance();
                installService(ctx, serviceName, instance);
            } catch (Exception e) {
                ROOT_LOGGER.cannotInstantiateClass(serviceName, e);
            }
        }
    }

    public abstract Class<T> getServiceClass();

    public abstract void installService(DeploymentPhaseContext ctx, String serviceName, T instance);

    @Override
    public void undeploy(DeploymentUnit context) {
        // Deploy only adds services, so no need to do anything here
        // since these services are automatically removed.
    }
}
