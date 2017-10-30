package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterConfiguration.Builder;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.server.infinispan.spi.service.CounterServiceName;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.value.InjectedValue;

/**
 * Add operation handler for /subsystem=infinispan/cache-container=clustered/counter=*
 *
 * @author Vladimir Blagojevic
 *
 */
public class CounterAddHandler extends AbstractAddStepHandler implements  RestartableServiceHandler{

    CounterAddHandler() {
    }

    @Override
    protected void performRuntime(OperationContext context, ModelNode operation, ModelNode model)
            throws OperationFailedException {

        super.performRuntime(context, operation, model);

        this.installRuntimeServices(context, operation, model, null);
    }

    @Override
    public Collection<ServiceController<?>> installRuntimeServices(OperationContext context, ModelNode operation,
            ModelNode containerModel, ModelNode cacheModel) throws OperationFailedException {

        String counterName = getCounterName(operation);
        String containerName = getContainerName(operation);
        String counterType = getCounterType(operation);

        Builder b = getBuilder(containerModel, counterType);
        processModelNode(context, containerModel, b);
        String name = CounterResource.COUNTER_NAME.resolveModelAttribute(context, containerModel).asString();
        if (!counterName.equals(name)) {
            throw new OperationFailedException("Counter node name and node's name attribute should be the same");
        }

        // wire counter service for this counter
        Collection<ServiceController<?>> controllers = new ArrayList<>(1);
        ServiceController<?> service = this.installCounterService(context.getServiceTarget(),
                containerName, counterName, b.build());
        controllers.add(service);

        return controllers;
    }

    @Override
    public void removeRuntimeServices(OperationContext context, ModelNode operation, ModelNode containerModel,
            ModelNode cacheModel) throws OperationFailedException {

        String counterName = getCounterName(operation);
        String containerName = getContainerName(operation);

        context.removeService(CounterServiceName.getServiceName(containerName, counterName));
    }

    private ServiceController<?> installCounterService(ServiceTarget target, String containerName,
            String configurationName, CounterConfiguration configuration) {
        final InjectedValue<EmbeddedCacheManager> container = new InjectedValue<>();
        final CounterConfigurationDependencies dependencies = new CounterConfigurationDependencies(container);
        final Service<?> service = new CounterService(configuration, configurationName, dependencies);
        final ServiceBuilder<?> builder = target.addService(CounterServiceName.getServiceName(containerName, configurationName), service)
                .addDependency(CacheContainerServiceName.CACHE_CONTAINER.getServiceName(containerName), EmbeddedCacheManager.class, container);
        return builder.install();
    }

    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
        this.populate(operation, model);
    }

    void populate(ModelNode fromModel, ModelNode toModel) throws OperationFailedException {
        for (AttributeDefinition attr : CounterResource.ATTRIBUTES) {
            attr.validateAndSet(fromModel, toModel);
        }
    }

    private String getCounterName(ModelNode operation) {
        PathAddress counterAddress = getCounterAddressFromOperation(operation);
        return counterAddress.getLastElement().getValue();
    }

    private String getContainerName(ModelNode operation) {
        PathAddress containerAddress = getCacheContainerAddressFromOperation(operation);
        return containerAddress.getLastElement().getValue();
    }

    private PathAddress getCacheContainerAddressFromOperation(ModelNode operation) {
        PathAddress counterAddress = getCounterAddressFromOperation(operation);
        return counterAddress.subAddress(0, counterAddress.size() - 2);
    }

    private String getCounterType(ModelNode operation) {
        PathAddress counterAddress = getCounterAddressFromOperation(operation);
        int size = counterAddress.size();
        PathAddress subAddress = counterAddress.subAddress(size - 1, size);
        return subAddress.getLastElement().getKey();
    }

    private PathAddress getCounterAddressFromOperation(ModelNode operation) {
        return PathAddress.pathAddress(operation.get(OP_ADDR));
    }

    private Builder getBuilder(ModelNode counter, String counterType) {
        boolean isWeakCounter = ModelKeys.WEAK_COUNTER.equals(counterType);
        if (isWeakCounter) {
            return CounterConfiguration.builder(CounterType.WEAK);
        } else {
            ModelNode lowerBoundModel = counter.get(ModelKeys.LOWER_BOUND);
            ModelNode upperBoundModel = counter.get(ModelKeys.UPPER_BOUND);
            boolean isBounded = lowerBoundModel.isDefined() || upperBoundModel.isDefined();
            return isBounded ? CounterConfiguration.builder(CounterType.BOUNDED_STRONG)
                    : CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG);
        }
    }

    void processModelNode(OperationContext context, ModelNode counter,
            CounterConfiguration.Builder builder) throws OperationFailedException {

        long initialValue = CounterResource.INITIAL_VALUE.resolveModelAttribute(context, counter).asLong();
        String storageType = CounterResource.STORAGE.resolveModelAttribute(context, counter).asString();

        builder.initialValue(initialValue);
        builder.storage(Storage.valueOf(storageType));
    }

    private static class CounterConfigurationDependencies implements CounterService.Dependencies {

        private InjectedValue<EmbeddedCacheManager> container;
        public CounterConfigurationDependencies(InjectedValue<EmbeddedCacheManager> container) {
           this.container = container;
        }

        @Override
        public EmbeddedCacheManager getCacheContainer() {
            return this.container.getValue();
        }

    }
}
