package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * @since 9.2
 */
public class KeyDataTypeConfigurationResource extends CacheConfigurationChildResource {

   public static final PathElement PATH = PathElement.pathElement(ModelKeys.ENCODING, ModelKeys.KEY);

   // attributes
   static final SimpleAttributeDefinition MEDIA_TYPE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MEDIA_TYPE, ModelType.STRING, true)
               .setXmlName(Attribute.MEDIA_TYPE.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_NONE)
               .setDefaultValue(new ModelNode().set(ContentTypeConfiguration.DEFAULT_MEDIA_TYPE))
               .build();

   static final AttributeDefinition[] ATTRIBUTES = {MEDIA_TYPE};

   public KeyDataTypeConfigurationResource(CacheConfigurationResource parent) {
      super(PATH, ModelKeys.ENCODING, parent, ATTRIBUTES);
   }

}
