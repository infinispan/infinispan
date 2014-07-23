package org.infinispan.server.endpoint.deployments;

import org.infinispan.filter.ConverterFactory;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.hotrod.HotRodServer;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class ConverterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<ConverterFactory> {

    private final ServiceName hotRodServerServiceName;

    public ConverterFactoryExtensionProcessor(ServiceName hotRodServerServiceName) {
        this.hotRodServerServiceName = hotRodServerServiceName;
    }

    @Override
    public ServiceBuilder<ConverterFactory> buildService(DeploymentPhaseContext ctx, String name, ConverterFactory instance) {
        ConverterFactoryService service = new ConverterFactoryService(name, instance);
        ServiceName serviceName = Constants.DATAGRID.append("converter-factory", name.replaceAll("\\.", "_"));
        ServiceBuilder<ConverterFactory> builder = ctx.getServiceTarget().addService(serviceName, service);
        builder.setInitialMode(ServiceController.Mode.ACTIVE)
                .addDependency(hotRodServerServiceName, ProtocolServer.class, service.getProtocolServer());
        return builder;
    }

    @Override
    public Class<ConverterFactory> getServiceClass() {
        return ConverterFactory.class;
    }

    private static class ConverterFactoryService implements Service<ConverterFactory> {

        private final ConverterFactory converterFactory;
        private final String name;
        private final InjectedValue<ProtocolServer> protocolServer = new InjectedValue<>();

        private ConverterFactoryService(String name, ConverterFactory converterFactory) {
            assert converterFactory != null : ROOT_LOGGER.nullVar("converterFactory");
            assert name != null : ROOT_LOGGER.nullVar("name");
            this.converterFactory = converterFactory;
            this.name = name;
        }

        @Override
        public void start(StartContext context) {
            ROOT_LOGGER.debugf("Started converter service with name = %s", name);
            ((HotRodServer) protocolServer.getValue()).addConverterFactory(name, converterFactory);
        }

        public InjectedValue<ProtocolServer> getProtocolServer() {
            return protocolServer;
        }

        @Override
        public void stop(StopContext context) {
            ROOT_LOGGER.debugf("Stopped converter service with name = %s", name);
        }

        @Override
        public ConverterFactory getValue() {
            return converterFactory;
        }
    }

}
