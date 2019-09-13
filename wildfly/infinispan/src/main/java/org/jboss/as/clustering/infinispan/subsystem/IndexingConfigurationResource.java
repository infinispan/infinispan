package org.jboss.as.clustering.infinispan.subsystem;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.infinispan.configuration.cache.IndexingConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.AttributeMarshaller;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleMapAttributeDefinition;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.operations.validation.EnumValidator;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.dmr.Property;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/indexing=INDEXING
 *
 * @author Tristan Tarrant
 */
public class IndexingConfigurationResource extends CacheConfigurationChildResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.INDEXING, ModelKeys.INDEXING_NAME);

    // attributes
    static final SimpleAttributeDefinition INDEXING =
          new SimpleAttributeDefinitionBuilder(ModelKeys.INDEXING, ModelType.STRING, true)
                .setXmlName(Attribute.INDEX.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setValidator(new EnumValidator<>(Indexing.class, true, false))
                .setDefaultValue(new ModelNode().set(Indexing.NONE.name()))
                .build();

    static final SimpleAttributeDefinition INDEXING_AUTO_CONFIG =
          new SimpleAttributeDefinitionBuilder(ModelKeys.AUTO_CONFIG, ModelType.BOOLEAN, true)
                .setXmlName(Attribute.AUTO_CONFIG.getLocalName())
                .setAllowExpression(true)
                .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                .setDefaultValue(new ModelNode().set(IndexingConfiguration.AUTO_CONFIG.getDefaultValue()))
                .build();

    static final StringListAttributeDefinition INDEXED_ENTITIES = new StringListAttributeDefinition.Builder(ModelKeys.INDEXED_ENTITIES)
          .setRequired(false)
          .setAllowExpression(false)
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .build();

    static final SimpleMapAttributeDefinition KEY_TRANSFORMERS = new SimpleMapAttributeDefinition.Builder(ModelKeys.KEY_TRANSFORMERS, true)
          .setRequired(false)
          .setAllowExpression(false)
          .setAttributeMarshaller(new AttributeMarshaller() {
              @Override
              public void marshallAsElement(AttributeDefinition attribute, ModelNode resourceModel, boolean marshallDefault, XMLStreamWriter writer) throws XMLStreamException {
                  resourceModel = resourceModel.get(attribute.getName());
                  if (!resourceModel.isDefined()) {
                      return;
                  }
                  for (Property property : resourceModel.asPropertyList()) {
                      writer.writeStartElement(Element.KEY_TRANSFORMER.getLocalName());
                      writer.writeAttribute(Attribute.KEY.getLocalName(), property.getName());
                      writer.writeAttribute(Attribute.TRANSFORMER.getLocalName(), property.getValue().asString());
                      writer.writeEndElement();
                  }
              }
          })
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .build();

    static final SimpleMapAttributeDefinition INDEXING_PROPERTIES = new SimpleMapAttributeDefinition.Builder(ModelKeys.INDEXING_PROPERTIES, true)
          .setAllowExpression(true)
          .setAttributeMarshaller(new AttributeMarshaller() {
              @Override
              public void marshallAsElement(AttributeDefinition attribute, ModelNode resourceModel, boolean marshallDefault, XMLStreamWriter writer) throws XMLStreamException {
                  resourceModel = resourceModel.get(attribute.getName());
                  if (!resourceModel.isDefined()) {
                      return;
                  }
                  for (Property property : resourceModel.asPropertyList()) {
                      writer.writeStartElement(org.jboss.as.controller.parsing.Element.PROPERTY.getLocalName());
                      writer.writeAttribute(org.jboss.as.controller.parsing.Element.NAME.getLocalName(), property.getName());
                      writer.writeCharacters(property.getValue().asString());
                      writer.writeEndElement();
                  }
              }
          })
          .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
          .build();

    private static final AttributeDefinition[] ATTRIBUTES = {INDEXING, INDEXING_AUTO_CONFIG, KEY_TRANSFORMERS, INDEXED_ENTITIES, INDEXING_PROPERTIES};

    public IndexingConfigurationResource(CacheConfigurationResource parent) {
        super(PATH, ModelKeys.INDEXING, parent, ATTRIBUTES);
    }
}
