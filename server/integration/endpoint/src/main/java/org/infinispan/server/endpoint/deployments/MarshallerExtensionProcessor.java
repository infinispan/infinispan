package org.infinispan.server.endpoint.deployments;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.endpoint.subsystem.ExtensionManagerService;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public class MarshallerExtensionProcessor extends AbstractServerExtensionProcessor<Marshaller> {

    private final ServiceName extensionManagerServiceName;

    public MarshallerExtensionProcessor(ServiceName extensionManagerServiceName) {
        this.extensionManagerServiceName = extensionManagerServiceName;
    }

    @Override
    public Class<Marshaller> getServiceClass() {
        return Marshaller.class;
    }

    @Override
    public void installService(DeploymentPhaseContext ctx, String serviceName, Marshaller instance) {
        MarshallerService service = new MarshallerService(instance);
        ServiceName extensionServiceName = Constants.DATAGRID.append(service.getServiceTypeName());
        ServiceBuilder<Marshaller> serviceBuilder = ctx.getServiceTarget().addService(extensionServiceName, service);
        serviceBuilder.setInitialMode(ServiceController.Mode.ACTIVE)
                .addDependency(extensionManagerServiceName, ExtensionManagerService.class, service.getExtensionManager());
        try {
            serviceBuilder.install();
        } catch (IllegalStateException e) {
            ROOT_LOGGER.duplicateMarshallerDeployment(ctx.getDeploymentUnit().getName());
            ROOT_LOGGER.debug("Marshaller already installed", e);
        }
    }

    static class MarshallerService implements Service<Marshaller> {
        private final Marshaller marshaller;
        private final InjectedValue<ExtensionManagerService> extensionManager = new InjectedValue<>();

        MarshallerService(Marshaller marshaller) {
            assert marshaller != null : ROOT_LOGGER.nullVar(getServiceTypeName());
            this.marshaller = marshaller;
        }

        public InjectedValue<ExtensionManagerService> getExtensionManager() {
            return extensionManager;
        }

        @Override
        public void start(StartContext context) throws StartException {
            ROOT_LOGGER.debugf("Started marshaller service with marshaller = %s", marshaller);
            extensionManager.getValue().setMarshaller(marshaller);
        }

        @Override
        public void stop(StopContext context) {
           ROOT_LOGGER.debugf("Stopped marshaller service with marshaller = %s", marshaller);
           extensionManager.getValue().setMarshaller(null);
        }

        @Override
        public Marshaller getValue() {
            return marshaller;
        }

        public String getServiceTypeName() {
            return "remote-event-marshaller";
        }
    }

}
