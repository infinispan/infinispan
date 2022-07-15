package org.infinispan.multimap.configuration;

import static org.infinispan.multimap.logging.Log.CONTAINER;

import org.infinispan.api.configuration.MultimapConfiguration;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@BuiltBy(EmbeddedMultimapConfigurationBuilder.class)
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_CONFIGURATION)
public class EmbeddedMultimapConfiguration implements MultimapConfiguration {

    public static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class)
          .validator(value -> {
              if (value == null || value.isBlank()) throw CONTAINER.missingMultimapName();
          }).immutable()
          .autoPersist(false)
          .build();
    public static final AttributeDefinition<Boolean> SUPPORTS_DUPLICATES = AttributeDefinition.builder(Attribute.SUPPORTS_DUPLICATES, false, Boolean.class)
            .immutable().build();

    final AttributeSet attributes;

    static AttributeSet attributeDefinitionSet() {
        return new AttributeSet(EmbeddedMultimapConfiguration.class, NAME, SUPPORTS_DUPLICATES);
    }

    EmbeddedMultimapConfiguration(AttributeSet attributes) {
        this.attributes = attributes;
    }

    @ProtoFactory
    EmbeddedMultimapConfiguration(String name, boolean supportsDuplicates) {
        AttributeSet attributes = attributeDefinitionSet();
        attributes.attribute(NAME).set(name);
        attributes.attribute(SUPPORTS_DUPLICATES).set(supportsDuplicates);
        this.attributes = attributes.protect();
    }

    @ProtoField(number = 2, defaultValue = "false")
    public boolean supportsDuplicates() {
        return attributes.attribute(SUPPORTS_DUPLICATES).get();
    }

    public AttributeSet attributes() {
        return attributes;
    }

    @ProtoField(number = 1)
    public String name() {
        return attributes.attribute(NAME).get();
    }
}
