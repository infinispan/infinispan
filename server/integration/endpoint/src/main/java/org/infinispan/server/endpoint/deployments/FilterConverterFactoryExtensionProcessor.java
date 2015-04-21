package org.infinispan.server.endpoint.deployments;

import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class FilterConverterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<CacheEventFilterConverterFactory> {

    public FilterConverterFactoryExtensionProcessor(ServiceName extensionManagerServiceName) {
        super(extensionManagerServiceName);
    }

    @Override
    public AbstractExtensionManagerService<CacheEventFilterConverterFactory> createService(String name, CacheEventFilterConverterFactory instance) {
        return new FilterConverterFactoryService(name, instance);
    }

    @Override
    public Class<CacheEventFilterConverterFactory> getServiceClass() {
        return CacheEventFilterConverterFactory.class;
    }

    private static class FilterConverterFactoryService extends AbstractExtensionManagerService<CacheEventFilterConverterFactory> {
        private FilterConverterFactoryService(String name, CacheEventFilterConverterFactory converterFactory) {
            super(name, converterFactory);
        }

        @Override
        public void start(StartContext context) {
            ROOT_LOGGER.debugf("Started combined filter and converter service with name = %s", name);
            extensionManager.getValue().addFilterConverterFactory(name, extension);
        }

        @Override
        public void stop(StopContext context) {
            ROOT_LOGGER.debugf("Stopped combined filter and converter service with name = %s", name);
            extensionManager.getValue().removeFilterConverterFactory(name);
        }

        @Override
        public CacheEventFilterConverterFactory getValue() {
            return extension;
        }

        @Override
        public String getServiceTypeName() {
            return "filter-converter-factory";
        }
    }

}
