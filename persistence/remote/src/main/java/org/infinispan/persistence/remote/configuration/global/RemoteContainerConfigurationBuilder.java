package org.infinispan.persistence.remote.configuration.global;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;
import static org.infinispan.persistence.remote.configuration.global.RemoteContainerConfiguration.NAME;
import static org.infinispan.persistence.remote.configuration.global.RemoteContainerConfiguration.URI;

import java.util.Properties;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * @since 15.0
 **/
public class RemoteContainerConfigurationBuilder implements Builder<RemoteContainerConfiguration> {

   private final AttributeSet attributes;

   public RemoteContainerConfigurationBuilder(GlobalConfigurationBuilder builder, String name) {
      this.attributes = new AttributeSet(RemoteContainerConfiguration.class,
            AbstractTypedPropertiesConfiguration.attributeSet(), NAME, URI);
      attributes.attribute(NAME).set(name);
   }

   @Override
   public RemoteContainerConfiguration create() {
      return new RemoteContainerConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(RemoteContainerConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RemoteContainerConfigurationBuilder uri(String uri) {
      attributes.attribute(URI).set(uri);
      return this;
   }

   public RemoteContainerConfigurationBuilder properties(Properties properties) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }
}
