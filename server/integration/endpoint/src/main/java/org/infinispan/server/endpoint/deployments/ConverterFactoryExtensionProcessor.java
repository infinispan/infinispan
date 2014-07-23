package org.infinispan.server.endpoint.deployments;

import org.infinispan.filter.ConverterFactory;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class ConverterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<ConverterFactory> {

    public ConverterFactoryExtensionProcessor(ServiceName extensionManagerServiceName) {
        super(extensionManagerServiceName);
    }

    @Override
    public AbstractExtensionManagerService<ConverterFactory> createService(String name, ConverterFactory instance) {
        return new ConverterFactoryService(name, instance);
    }

    @Override
    public Class<ConverterFactory> getServiceClass() {
        return ConverterFactory.class;
    }

    private static class ConverterFactoryService extends AbstractExtensionManagerService<ConverterFactory> {
        private ConverterFactoryService(String name, ConverterFactory converterFactory) {
            super(name, converterFactory);
        }

        @Override
        public void start(StartContext context) {
            ROOT_LOGGER.debugf("Started converter service with name = %s", name);
            extensionManager.getValue().addConverterFactory(name, extension);
        }

        @Override
        public void stop(StopContext context) {
            ROOT_LOGGER.debugf("Stopped converter service with name = %s", name);
        }

        @Override
        public ConverterFactory getValue() {
            return extension;
        }

        @Override
        public String getServiceTypeName() {
            return "converter-factory";
        }
    }

}
