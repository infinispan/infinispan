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
 * @author Tristan Tarrant
 */
public class LevelDBCompressionResource extends CacheChildResource {

    public static final PathElement LEVELDB_COMPRESSION_PATH = PathElement.pathElement(ModelKeys.COMPRESSION, ModelKeys.COMPRESSION_NAME);

    static final SimpleAttributeDefinition TYPE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.TYPE, ModelType.STRING, true)
                    .setXmlName(Attribute.TYPE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(CompressionType.class, true, false))
                    .setDefaultValue(new ModelNode().set(CompressionType.NONE.name()))
                    .build();

    static final AttributeDefinition[] LEVELDB_COMPRESSION_ATTRIBUTES = {TYPE};


    public LevelDBCompressionResource(CacheResource cacheResource) {
        super(LEVELDB_COMPRESSION_PATH, ModelKeys.COMPRESSION, cacheResource, LEVELDB_COMPRESSION_ATTRIBUTES);
    }

}
