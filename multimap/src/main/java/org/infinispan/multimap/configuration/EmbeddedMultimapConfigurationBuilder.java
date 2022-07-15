package org.infinispan.multimap.configuration;

import static org.infinispan.multimap.configuration.EmbeddedMultimapConfiguration.NAME;
import static org.infinispan.multimap.configuration.EmbeddedMultimapConfiguration.SUPPORTS_DUPLICATES;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.ConfigurationBuilder;

public class EmbeddedMultimapConfigurationBuilder implements Builder<EmbeddedMultimapConfiguration>, Self<EmbeddedMultimapConfigurationBuilder> {

    private final AttributeSet attributes = EmbeddedMultimapConfiguration.attributeDefinitionSet();

    public EmbeddedMultimapConfigurationBuilder(ConfigurationBuilder ignore) { }

    public EmbeddedMultimapConfigurationBuilder() { }

    @Override
    public void validate() {
        attributes.attributes().forEach(Attribute::validate);
    }

    @Override
    public EmbeddedMultimapConfiguration create() {
        return new EmbeddedMultimapConfiguration(attributes.protect());
    }

    @Override
    public Builder<?> read(EmbeddedMultimapConfiguration template) {
        this.attributes.read(template.attributes());
        return this;
    }

    @Override
    public AttributeSet attributes() {
        return attributes;
    }

    public EmbeddedMultimapConfigurationBuilder name(String name) {
        attributes.attribute(NAME).set(name);
        return self();
    }

    public String name() {
        return attributes.attribute(NAME).get();
    }

    public EmbeddedMultimapConfigurationBuilder supportsDuplicates(boolean supportsDuplicates) {
        attributes.attribute(SUPPORTS_DUPLICATES).set(supportsDuplicates);
        return self();
    }

    @Override
    public EmbeddedMultimapConfigurationBuilder self() {
        return this;
    }
}
