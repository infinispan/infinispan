package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

public class RemoteServerConfigurationBuilder extends AbstractRemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> implements
      Builder<RemoteServerConfiguration> {
   private String host;
   private int port = 11222;

   RemoteServerConfigurationBuilder(RemoteStoreConfigurationBuilder builder) {
      super(builder);
   }

   public RemoteServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public RemoteServerConfigurationBuilder port(int port) {
      this.port = port;
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
      return new RemoteServerConfiguration(host, port);
   }

   @Override
   public RemoteServerConfigurationBuilder read(RemoteServerConfiguration template) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }
}
