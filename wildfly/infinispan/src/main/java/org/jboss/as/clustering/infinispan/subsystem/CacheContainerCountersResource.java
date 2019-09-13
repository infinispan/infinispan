package org.jboss.as.clustering.infinispan.subsystem;

import java.util.EnumSet;

import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * CacheContainerCountersResource
 *
 * @author Vladimir Blagojevic
 * @since 9.2
 */
public class CacheContainerCountersResource extends SimpleResourceDefinition {
    static final PathElement PATH = PathElement.pathElement(ModelKeys.COUNTERS, ModelKeys.COUNTERS_NAME);

    //attributes
    static final SimpleAttributeDefinition RELIABILITY = new SimpleAttributeDefinitionBuilder(ModelKeys.RELIABILITY,
            ModelType.STRING, false)
            .setXmlName(Attribute.RELIABILITY.getLocalName())
            .setAllowExpression(false)
            .setValidator(new EnumValidator<>(Reliability.class, EnumSet.allOf(Reliability.class)))
            .setDefaultValue(new ModelNode().set(CounterManagerConfigurationBuilder.defaultConfiguration().reliability().name()))
            .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setRequired(false)
            .build();

    static final SimpleAttributeDefinition NUM_OWNERS = new SimpleAttributeDefinitionBuilder(ModelKeys.NUM_OWNERS,
            ModelType.LONG, false)
            .setXmlName(Attribute.NUM_OWNERS.getLocalName())
            .setAllowExpression(false)
            .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
            .setDefaultValue(new ModelNode().set(CounterManagerConfigurationBuilder.defaultConfiguration().numOwners()))
            .setRequired(false)
            .build();

    static final AttributeDefinition[] ATTRIBUTES = {RELIABILITY, NUM_OWNERS};

    private final boolean runtimeRegistration;

    CacheContainerCountersResource(ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(PATH, new InfinispanResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER, ModelKeys.COUNTERS),
              new ReloadRequiredAddStepHandler(ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerChildren(ManagementResourceRegistration rr) {
        rr.registerSubModel(new StrongCounterResource(runtimeRegistration));
        rr.registerSubModel(new WeakCounterResource(runtimeRegistration));
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
        for (AttributeDefinition attr : ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }
}
