package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.RemoteServerConfiguration.HOST;
import static org.infinispan.persistence.remote.configuration.RemoteServerConfiguration.PORT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class RemoteServerConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements
      Builder<RemoteServerConfiguration> {

   RemoteServerConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder, RemoteServerConfiguration.attributeDefinitionSet());
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public RemoteServerConfigurationBuilder host(String host) {
      this.attributes.attribute(HOST).set(host);
      return this;
   }

   public RemoteServerConfigurationBuilder port(int port) {
      this.attributes.attribute(PORT).set(port);
      return this;
   }

   @Override
   public RemoteServerConfiguration create() {
      return new RemoteServerConfiguration(attributes.protect());
   }

   @Override
   public RemoteServerConfigurationBuilder read(RemoteServerConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
