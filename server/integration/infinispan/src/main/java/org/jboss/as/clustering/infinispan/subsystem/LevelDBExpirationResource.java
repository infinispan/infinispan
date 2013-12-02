package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.*;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 *
 *    /subsystem=infinispan/cache-container=X/cache=Y/store=Z/expiration=EXPIRATION
 *
 * @author Galder Zamarreño
 */
public class LevelDBExpirationResource extends SimpleResourceDefinition {

    public static final PathElement LEVELDB_EXPIRATION_PATH = PathElement.pathElement(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);

    static final SimpleAttributeDefinition PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
                    .setXmlName(Attribute.PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .build();

    static final SimpleAttributeDefinition RELATIVE_TO =
            new SimpleAttributeDefinitionBuilder(ModelKeys.RELATIVE_TO, ModelType.STRING, true)
                    .setXmlName(Attribute.RELATIVE_TO.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(ServerEnvironment.SERVER_DATA_DIR))
                    .build();

    static final SimpleAttributeDefinition QUEUE_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.QUEUE_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.QUEUE_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setDefaultValue(new ModelNode().set(10000))
                    .build();

    static final AttributeDefinition[] LEVELDB_EXPIRATION_ATTRIBUTES = {PATH, QUEUE_SIZE};

    public LevelDBExpirationResource() {
        super(LEVELDB_EXPIRATION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.EXPIRATION),
                CacheConfigOperationHandlers.LEVELDB_EXPIRATION_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(LEVELDB_EXPIRATION_ATTRIBUTES);
        for (AttributeDefinition attr : LEVELDB_EXPIRATION_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

}
