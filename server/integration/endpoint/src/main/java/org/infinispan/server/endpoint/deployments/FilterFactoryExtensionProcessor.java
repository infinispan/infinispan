package org.infinispan.server.endpoint.deployments;

import org.infinispan.filter.KeyValueFilterFactory;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StopContext;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

public final class FilterFactoryExtensionProcessor extends AbstractNamedFactoryExtensionProcessor<KeyValueFilterFactory> {

    public FilterFactoryExtensionProcessor(ServiceName extensionManagerServiceName) {
        super(extensionManagerServiceName);
    }

    @Override
    public Class<KeyValueFilterFactory> getServiceClass() {
        return KeyValueFilterFactory.class;
    }

    @Override
    public AbstractExtensionManagerService<KeyValueFilterFactory> createService(String name, KeyValueFilterFactory instance) {
        return new FilterFactoryService(name, instance);
    }

    private static class FilterFactoryService extends AbstractExtensionManagerService<KeyValueFilterFactory> {
        public FilterFactoryService(String name, KeyValueFilterFactory filterFactory) {
            super(name, filterFactory);
        }

        @Override
        public void start(StartContext context) {
            ROOT_LOGGER.debugf("Started key-value filter service with name = %s", name);
            extensionManager.getValue().addKeyValueFilterFactory(name, extension);
        }

        @Override
        public String getServiceTypeName() {
            return "key-value-filter-factory";
        }

        @Override
        public void stop(StopContext context) {
            ROOT_LOGGER.debugf("Stopped key-value filter service with name = %s", name);
            extensionManager.getValue().removeKeyValueFilterFactory(name);
        }

        @Override
        public KeyValueFilterFactory getValue() {
            return extension;
        }
    }

}
