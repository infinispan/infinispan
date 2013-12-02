package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.persistence.leveldb.configuration.CompressionType;
import org.jboss.as.controller.*;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 *
 *    /subsystem=infinispan/cache-container=X/cache=Y/store=Z/expiration=COMPRESSION
 *
 * @author Galder Zamarreño
 */
public class LevelDBCompressionResource extends SimpleResourceDefinition {

    public static final PathElement LEVELDB_COMPRESSION_PATH = PathElement.pathElement(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME);

    static final SimpleAttributeDefinition TYPE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TYPE, ModelType.STRING, true)
                    .setXmlName(Attribute.TYPE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .setValidator(new EnumValidator<CompressionType>(CompressionType.class, true, false))
                    .setDefaultValue(new ModelNode().set(CompressionType.NONE.name()))
                    .build();

    static final AttributeDefinition[] LEVELDB_COMPRESSION_ATTRIBUTES = {TYPE};


    public LevelDBCompressionResource() {
        super(LEVELDB_COMPRESSION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.COMPRESSION),
                CacheConfigOperationHandlers.LEVELDB_COMPRESSION_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(LEVELDB_COMPRESSION_ATTRIBUTES);
        for (AttributeDefinition attr : LEVELDB_COMPRESSION_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

}
