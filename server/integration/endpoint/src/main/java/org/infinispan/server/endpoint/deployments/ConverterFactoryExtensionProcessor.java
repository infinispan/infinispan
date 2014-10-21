package org.infinispan.server.endpoint.deployments;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class ConverterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<CacheEventConverterFactory> {

    public ConverterFactoryExtensionProcessor(ServiceName extensionManagerServiceName) {
        super(extensionManagerServiceName);
    }

    @Override
    public AbstractExtensionManagerService<CacheEventConverterFactory> createService(String name, CacheEventConverterFactory instance) {
        return new ConverterFactoryService(name, instance);
    }

    @Override
    public Class<CacheEventConverterFactory> getServiceClass() {
        return CacheEventConverterFactory.class;
    }

    private static class ConverterFactoryService extends AbstractExtensionManagerService<CacheEventConverterFactory> {
        private ConverterFactoryService(String name, CacheEventConverterFactory converterFactory) {
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
            extensionManager.getValue().removeConverterFactory(name);
        }

        @Override
        public CacheEventConverterFactory getValue() {
            return extension;
        }

        @Override
        public String getServiceTypeName() {
            return "converter-factory";
        }
    }

}
