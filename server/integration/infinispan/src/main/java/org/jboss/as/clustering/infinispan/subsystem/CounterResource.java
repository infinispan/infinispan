package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.PathAddress.pathAddress;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;

import java.util.Optional;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.AbstractRemoveStepHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 * /subsystem=infinispan/cache-container=X/counter=COUNTERS
 *
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class CounterResource extends SimpleResourceDefinition {

    //attributes
    static final SimpleAttributeDefinition COUNTER_NAME = new SimpleAttributeDefinitionBuilder(ModelKeys.NAME,
          ModelType.STRING, false)
          .setXmlName(Attribute.NAME.getLocalName())
          .setAllowExpression(false)
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .build();

    static final SimpleAttributeDefinition STORAGE = new SimpleAttributeDefinitionBuilder(ModelKeys.STORAGE,
          ModelType.STRING, false)
          .setXmlName(Attribute.STORAGE.getLocalName())
          .setAllowExpression(false)
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .setAllowedValues(Storage.VOLATILE.toString(), Storage.PERSISTENT.toString())
          .setDefaultValue(new ModelNode().set(Storage.VOLATILE.toString()))
          .build();

    static final SimpleAttributeDefinition INITIAL_VALUE = new SimpleAttributeDefinitionBuilder(ModelKeys.INITIAL_VALUE,
          ModelType.LONG, true)
          .setXmlName(Attribute.INITIAL_VALUE.getLocalName())
          .setAllowExpression(false)
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .setDefaultValue(new ModelNode().set(0))
          .build();

    static final AttributeDefinition[] ATTRIBUTES = { COUNTER_NAME, STORAGE, INITIAL_VALUE };

    // operations

    private static final OperationDefinition COUNTER_RESET = buildOperation("counter-reset").build();
    private static final OperationDefinition COUNTER_INCREASE = buildOperation("counter-increase").build();
    private static final OperationDefinition COUNTER_DECREASE = buildOperation("counter-decrease").build();

    private final boolean runtimeRegistration;

    public CounterResource(PathElement pathElement, ResourceDescriptionResolver descriptionResolver,
            AbstractAddStepHandler addHandler, OperationStepHandler removeHandler, boolean runtimeRegistration) {
        super(pathElement, descriptionResolver, addHandler, removeHandler);
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);
        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadOnlyAttribute(attr, null);
        }

        if (runtimeRegistration) {
            CounterMetricsHandler.INSTANCE.registerMetrics(resourceRegistration);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        if (runtimeRegistration) {
            resourceRegistration.registerOperationHandler(CounterResource.COUNTER_RESET, CounterResetCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CounterResource.COUNTER_INCREASE,
                    CounterIncreaseCommand.INSTANCE);
            resourceRegistration.registerOperationHandler(CounterResource.COUNTER_DECREASE,
                    CounterDecreaseCommand.INSTANCE);
        }
    }

    private static SimpleOperationDefinitionBuilder buildOperation(String name) {
        return new SimpleOperationDefinitionBuilder(name, new InfinispanResourceDescriptionResolver(ModelKeys.COUNTERS))
                .setRuntimeOnly();
    }

    private static PathElement counterElement(ModelNode operation) {
        final PathAddress address = pathAddress(operation.require(OP_ADDR));
        final PathElement counterElement = address.getElement(address.size() - 1);
        return counterElement;
    }

    private static String counterName(ModelNode operation) {
        PathElement counterElement = counterElement(operation);
        return counterElement.getValue();
    }

    private static String counterType(ModelNode operation) {
        PathElement counterElement = counterElement(operation);
        return counterElement.getKey();
    }

    private static OperationFailedException counterManagerNotFound() {
        return new OperationFailedException("CounterManager not found in server.");
    }

    public static class CounterRemoveHandler extends AbstractRemoveStepHandler {

        private final CounterAddHandler handler;

        public CounterRemoveHandler() {
            handler = new CounterAddHandler();
        }

        @Override
        protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model) throws OperationFailedException {
            handler.removeRuntimeServices(context, operation, null, null);
        }

        @Override
        protected void recoverServices(final OperationContext context, final ModelNode operation, final ModelNode model) throws OperationFailedException {
            handler.performRuntime(context, operation, model);
        }
    }

    private static class CounterResetCommand extends BaseCounterManagerCommand {
        private static final CounterResetCommand INSTANCE = new CounterResetCommand();

        @Override
        protected ModelNode invoke(CounterManager counterManager, ModelNode operation)
                throws Exception {
            final String counterName = counterName(operation);
            final String counterType = counterType(operation);
            if (counterManager.isDefined(counterName)) {
                boolean isStrongCounter = ModelKeys.STRONG_COUNTER.equals(counterType);
                if (isStrongCounter) {
                    StrongCounter strongCounter = counterManager.getStrongCounter(counterName);
                    strongCounter.sync().reset();
                } else {
                    WeakCounter weakCounter = counterManager.getWeakCounter(counterName);
                    weakCounter.sync().reset();
                }
            }
            return new ModelNode();
        }
    }

    private static class CounterIncreaseCommand extends BaseCounterManagerCommand {
        private static final CounterIncreaseCommand INSTANCE = new CounterIncreaseCommand();

        @Override
        protected ModelNode invoke(CounterManager counterManager, ModelNode operation)
                throws Exception {
            final String counterName = counterName(operation);
            final String counterType = counterType(operation);
            if (counterManager.isDefined(counterName)) {
                boolean isStrongCounter = ModelKeys.STRONG_COUNTER.equals(counterType);
                if (isStrongCounter) {
                    StrongCounter strongCounter = counterManager.getStrongCounter(counterName);
                    strongCounter.sync().incrementAndGet();
                } else {
                    WeakCounter weakCounter = counterManager.getWeakCounter(counterName);
                    weakCounter.sync().increment();
                }
            }
            return new ModelNode();
        }
    }

    private static class CounterDecreaseCommand extends BaseCounterManagerCommand {
        private static final CounterDecreaseCommand INSTANCE = new CounterDecreaseCommand();

        @Override
        protected ModelNode invoke(CounterManager counterManager, ModelNode operation)
                throws Exception {
            final String counterName = counterName(operation);
            final String counterType = counterType(operation);
            if (counterManager.isDefined(counterName)) {
                boolean isStrongCounter = ModelKeys.STRONG_COUNTER.equals(counterType);
                if (isStrongCounter) {
                    StrongCounter strongCounter = counterManager.getStrongCounter(counterName);
                    strongCounter.sync().decrementAndGet();
                } else {
                    WeakCounter weakCounter = counterManager.getWeakCounter(counterName);
                    weakCounter.sync().decrement();
                }
            }
            return new ModelNode();
        }
    }

    private static abstract class BaseCounterManagerCommand extends CacheContainerCommands {

        BaseCounterManagerCommand() {
            //path to container from counter address has two elements
            super(2);
        }

        abstract ModelNode invoke(CounterManager counterManager, ModelNode operation)
                throws Exception;

        @Override
        protected final ModelNode invokeCommand(EmbeddedCacheManager cacheManager, OperationContext context,
                ModelNode operation) throws Exception {
            Optional<CounterManager> optCounterManager = SecurityActions.findCounterManager(cacheManager);
            CounterManager counterManager = optCounterManager.orElseThrow(CounterResource::counterManagerNotFound);
            return invoke(counterManager, operation);
        }
    }
}
