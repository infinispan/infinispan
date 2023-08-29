package org.infinispan.persistence.remote.configuration.global;

import static org.infinispan.persistence.remote.configuration.global.RemoteContainerConfiguration.NAME;
import static org.infinispan.persistence.remote.configuration.global.RemoteContainerConfiguration.URI;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

/**
 * @since 15.0
 **/
public class RemoteContainerConfigurationBuilder implements Builder<RemoteContainerConfiguration> {

   private final AttributeSet attributes;

   public RemoteContainerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.attributes = RemoteContainerConfiguration.attributeSet();
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

   public RemoteContainerConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   public RemoteContainerConfigurationBuilder uri(String uri) {
      attributes.attribute(URI).set(uri);
      return this;
   }
}
