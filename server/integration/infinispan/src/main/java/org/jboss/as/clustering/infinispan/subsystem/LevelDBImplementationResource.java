package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 *
 *    /subsystem=infinispan/cache-container=X/cache=Y/store=Z/implementation=IMPLEMENTATION
 *
 * @author Galder Zamarreño
 */
public class LevelDBImplementationResource extends SimpleResourceDefinition {

    public static final PathElement LEVELDB_IMPLEMENTATION_PATH = PathElement.pathElement(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME);

    static final SimpleAttributeDefinition TYPE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.IMPLEMENTATION, ModelType.STRING, true)
                    .setXmlName(Attribute.TYPE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setValidator(new EnumValidator<LevelDBStoreConfiguration.ImplementationType>(LevelDBStoreConfiguration.ImplementationType.class, true, false))
                    .setDefaultValue(new ModelNode().set(LevelDBStoreConfiguration.ImplementationType.AUTO.name()))
                    .build();

    static final AttributeDefinition[] LEVELDB_IMPLEMENTATION_ATTRIBUTES = {TYPE};


    public LevelDBImplementationResource() {
        super(LEVELDB_IMPLEMENTATION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.IMPLEMENTATION),
                CacheConfigOperationHandlers.LEVELDB_IMPLEMENTATION_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(LEVELDB_IMPLEMENTATION_ATTRIBUTES);
        for (AttributeDefinition attr : LEVELDB_IMPLEMENTATION_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

}
