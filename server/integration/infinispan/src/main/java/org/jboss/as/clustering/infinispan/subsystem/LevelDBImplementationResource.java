package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 *
 *    /subsystem=infinispan/cache-container=X/cache=Y/store=Z/implementation=IMPLEMENTATION
 *
 * @author Galder Zamarreño
 * @author Tristan Tarrant
 */
public class LevelDBImplementationResource extends CacheChildResource {

    public static final PathElement LEVELDB_IMPLEMENTATION_PATH = PathElement.pathElement(ModelKeys.IMPLEMENTATION, ModelKeys.IMPLEMENTATION_NAME);

    static final SimpleAttributeDefinition TYPE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.IMPLEMENTATION, ModelType.STRING, true)
                    .setXmlName(Attribute.TYPE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setValidator(new EnumValidator<>(LevelDBStoreConfiguration.ImplementationType.class, true, false))
                    .setDefaultValue(new ModelNode().set(LevelDBStoreConfiguration.ImplementationType.AUTO.name()))
                    .build();

    static final AttributeDefinition[] LEVELDB_IMPLEMENTATION_ATTRIBUTES = {TYPE};


    public LevelDBImplementationResource(CacheResource cacheResource) {
        super(LEVELDB_IMPLEMENTATION_PATH, ModelKeys.IMPLEMENTATION, cacheResource, LEVELDB_IMPLEMENTATION_ATTRIBUTES);
    }

}
