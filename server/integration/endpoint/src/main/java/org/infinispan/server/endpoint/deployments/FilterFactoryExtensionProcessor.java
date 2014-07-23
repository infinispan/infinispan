package org.infinispan.server.endpoint.deployments;

import org.infinispan.filter.KeyValueFilterFactory;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.hotrod.HotRodServer;
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

public final class FilterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<KeyValueFilterFactory> {

    private final ServiceName hotRodServerServiceName;

    public FilterFactoryExtensionProcessor(ServiceName hotRodServerServiceName) {
        this.hotRodServerServiceName = hotRodServerServiceName;
    }

    @Override
    public ServiceBuilder<KeyValueFilterFactory> buildService(DeploymentPhaseContext ctx, String name, KeyValueFilterFactory instance) {
        FilterFactoryService service = new FilterFactoryService(name, instance);
        ServiceName serviceName = Constants.DATAGRID.append("key-value-filter-factory", name.replaceAll("\\.", "_"));
        ServiceBuilder<KeyValueFilterFactory> builder = ctx.getServiceTarget().addService(serviceName, service);
        builder.setInitialMode(ServiceController.Mode.ACTIVE)
                .addDependency(hotRodServerServiceName, ProtocolServer.class, service.getProtocolServer());
        return builder;
    }

    @Override
    public Class<KeyValueFilterFactory> getServiceClass() {
        return KeyValueFilterFactory.class;
    }

    private static class FilterFactoryService implements Service<KeyValueFilterFactory> {

        private final KeyValueFilterFactory filterFactory;
        private final String name;
        private final InjectedValue<ProtocolServer> protocolServer = new InjectedValue<>();

        public FilterFactoryService(String name, KeyValueFilterFactory filterFactory) {
            assert filterFactory != null : ROOT_LOGGER.nullVar("filterFactory");
            assert name != null : ROOT_LOGGER.nullVar("name");
            this.filterFactory = filterFactory;
            this.name = name;
        }

        @Override
        public void start(StartContext context) {
            ROOT_LOGGER.debugf("Started key-value filter service with name = %s", name);
            ((HotRodServer) protocolServer.getValue()).addKeyValueFilterFactory(name, filterFactory);
        }

        public InjectedValue<ProtocolServer> getProtocolServer() {
            return protocolServer;
        }

        @Override
        public void stop(StopContext context) {
            ROOT_LOGGER.debugf("Stopped key-value filter service with name = %s", name);
        }

        @Override
        public KeyValueFilterFactory getValue() {
            return filterFactory;
        }
    }

}
