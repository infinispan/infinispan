package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.RemoteServerConfiguration.HOST;
import static org.infinispan.persistence.remote.configuration.RemoteServerConfiguration.PORT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

public class RemoteServerConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements
      Builder<RemoteServerConfiguration> {

   RemoteServerConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder, RemoteServerConfiguration.attributeDefinitionSet());
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
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public RemoteServerConfiguration create() {
      return new RemoteServerConfiguration(attributes.protect());
   }

   @Override
   public RemoteServerConfigurationBuilder read(RemoteServerConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
