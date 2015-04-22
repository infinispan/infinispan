package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource
 *
 *    /subsystem=infinispan/cache-container=X/cache=Y/store=Z/expiration=EXPIRATION
 *
 * @author Galder Zamarreño
 */
public class LevelDBExpirationResource extends CacheChildResource {

    public static final PathElement LEVELDB_EXPIRATION_PATH = PathElement.pathElement(ModelKeys.EXPIRATION, ModelKeys.EXPIRATION_NAME);

    static final SimpleAttributeDefinition PATH =
            new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
                    .setXmlName(Attribute.PATH.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final SimpleAttributeDefinition QUEUE_SIZE =
            new SimpleAttributeDefinitionBuilder(ModelKeys.QUEUE_SIZE, ModelType.INT, true)
                    .setXmlName(Attribute.QUEUE_SIZE.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(10000))
                    .build();

    static final AttributeDefinition[] LEVELDB_EXPIRATION_ATTRIBUTES = {PATH, QUEUE_SIZE};

    public LevelDBExpirationResource(CacheResource cacheResource) {
        super(LEVELDB_EXPIRATION_PATH, ModelKeys.EXPIRATION, cacheResource, LEVELDB_EXPIRATION_ATTRIBUTES);
    }

}
