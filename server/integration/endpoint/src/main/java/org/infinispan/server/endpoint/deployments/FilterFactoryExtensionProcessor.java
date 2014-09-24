package org.infinispan.server.endpoint.deployments;

import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class FilterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<CacheEventFilterFactory> {

    public FilterFactoryExtensionProcessor(ServiceName extensionManagerServiceName) {
        super(extensionManagerServiceName);
    }

    @Override
    public Class<CacheEventFilterFactory> getServiceClass() {
        return CacheEventFilterFactory.class;
    }

    @Override
    public AbstractExtensionManagerService<CacheEventFilterFactory> createService(String name, CacheEventFilterFactory instance) {
        return new FilterFactoryService(name, instance);
    }

    private static class FilterFactoryService extends AbstractExtensionManagerService<CacheEventFilterFactory> {
        public FilterFactoryService(String name, CacheEventFilterFactory filterFactory) {
            super(name, filterFactory);
        }

        @Override
        public void start(StartContext context) {
            ROOT_LOGGER.debugf("Started key-value filter service with name = %s", name);
            extensionManager.getValue().addFilterFactory(name, extension);
        }

        @Override
        public String getServiceTypeName() {
            return "key-value-filter-factory";
        }

        @Override
        public void stop(StopContext context) {
            ROOT_LOGGER.debugf("Stopped key-value filter service with name = %s", name);
            extensionManager.getValue().removeFilterFactory(name);
        }

        @Override
        public CacheEventFilterFactory getValue() {
            return extension;
        }
    }

}
