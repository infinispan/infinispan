package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
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
public class CounterMetricsHandler extends AbstractRuntimeOnlyHandler {

    public static final CounterMetricsHandler INSTANCE = new CounterMetricsHandler();

    private static final int CACHE_CONTAINER_INDEX = 1;
    private static final int COUNTER_INDEX = 3;

    @Override
    protected void executeRuntimeStep(OperationContext context, ModelNode operation) {

        final ModelNode result = new ModelNode();
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(CACHE_CONTAINER_INDEX).getValue();
        final String counterType = address.getElement(COUNTER_INDEX).getKey();
        final String counterName = address.getElement(COUNTER_INDEX).getValue();
        final ServiceController<?> controller = context.getServiceRegistry(false)
                .getService(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));

        Long value;
        if (controller != null) {
            DefaultCacheContainer cacheManager = (DefaultCacheContainer) controller.getValue();
            CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);
            if (ModelKeys.STRONG_COUNTER.equals(counterType)) {
                StrongCounter sc = counterManager.getStrongCounter(counterName);
                value = sc.sync().getValue();
            } else {
                WeakCounter wc = counterManager.getWeakCounter(counterName);
                value = wc.sync().getValue();
            }
            result.set(value);
        }
        context.getResult().set(result);
    }

    public void registerMetrics(ManagementResourceRegistration container) {
        for (CounterMetrics metric : CounterMetrics.values()) {
            container.registerMetric(metric.definition, this);
        }
    }

    public enum CounterMetrics {

        VALUE(MetricKeys.VALUE, ModelType.LONG);

        final AttributeDefinition definition;

        CounterMetrics(String attributeName, ModelType type) {
            this.definition = new SimpleAttributeDefinitionBuilder(attributeName, type).setRequired(true)
                    .setStorageRuntime().build();
        }

        @Override
        public final String toString() {
            return definition.getName();
        }
    }
}
