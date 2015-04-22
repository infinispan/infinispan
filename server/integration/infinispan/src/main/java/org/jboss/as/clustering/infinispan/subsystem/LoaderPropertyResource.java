package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelType;

/**
 * @author Tristan Tarrant
 */
public class LoaderPropertyResource extends CacheChildResource {

    static final PathElement LOADER_PROPERTY_PATH = PathElement.pathElement(ModelKeys.PROPERTY);

    // attributes
    static final SimpleAttributeDefinition VALUE =
            new SimpleAttributeDefinitionBuilder("value", ModelType.STRING, false)
                    .setXmlName("value")
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .build();

    static final AttributeDefinition[] LOADER_PROPERTY_ATTRIBUTES = {VALUE};

    public LoaderPropertyResource(CacheResource cacheResource) {
        super(LOADER_PROPERTY_PATH, ModelKeys.PROPERTY, cacheResource, LOADER_PROPERTY_ATTRIBUTES);
    }

}
