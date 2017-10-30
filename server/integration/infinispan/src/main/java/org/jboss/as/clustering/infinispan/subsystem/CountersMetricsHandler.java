package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;

/**
 * Reflects runtime Counter attributes
 *
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class CountersMetricsHandler extends AbstractRuntimeOnlyHandler {

    public static final CountersMetricsHandler INSTANCE = new CountersMetricsHandler();

    private static final int CACHE_CONTAINER_INDEX = 1;

    @Override
    protected void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
        ModelNode attributeRead = operation.require(NAME);
        final ModelNode result = new ModelNode();
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(CACHE_CONTAINER_INDEX).getValue();
        final ServiceController<?> controller = context.getServiceRegistry(false)
                .getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));

        String attribute = attributeRead.asString();
        if (controller != null) {
            DefaultCacheContainer cacheManager = (DefaultCacheContainer) controller.getValue();
            CounterManagerConfiguration counterManagerConfiguration = extractCounterManagerConfiguration(cacheManager);

            //we only have two attributes in CounterManagerConfiguration
            if (CounterConfigurationMetrics.NUM_OF_OWNERS.toString().equals(attribute)) {
                result.set(counterManagerConfiguration.numOwners());
            } else if (CounterConfigurationMetrics.RELIABILITY.toString().equals(attribute)) {
                result.set(counterManagerConfiguration.reliability().name());
            }
        }
        context.getResult().set(result);
    }

    public void registerMetrics(ManagementResourceRegistration container) {
        for (CounterConfigurationMetrics metric : CounterConfigurationMetrics.values()) {
            container.registerMetric(metric.definition, this);
        }
    }

    private CounterManagerConfiguration extractCounterManagerConfiguration(EmbeddedCacheManager cacheManager) {
        GlobalComponentRegistry globalComponentRegistry = SecurityActions.getGlobalComponentRegistry(cacheManager);
        CounterManagerConfiguration config = globalComponentRegistry.getGlobalConfiguration()
                .module(CounterManagerConfiguration.class);
        return config == null ? CounterManagerConfigurationBuilder.defaultConfiguration() : config;
    }

    public enum CounterConfigurationMetrics {

        NUM_OF_OWNERS(MetricKeys.NUM_OF_OWNERS, ModelType.LONG),
        RELIABILITY(MetricKeys.RELIABILITY, ModelType.STRING);

        final AttributeDefinition definition;

        CounterConfigurationMetrics(String attributeName, ModelType type) {
            this.definition = new SimpleAttributeDefinitionBuilder(attributeName, type).setAllowNull(false)
                    .setStorageRuntime().build();
        }

        @Override
        public final String toString() {
            return definition.getName();
        }
    }
}
