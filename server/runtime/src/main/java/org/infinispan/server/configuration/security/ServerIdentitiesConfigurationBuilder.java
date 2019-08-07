package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class ServerIdentitiesConfigurationBuilder implements Builder<ServerIdentitiesConfiguration> {
   private final List<SSLConfigurationBuilder> sslConfigurations = new ArrayList<>();
   private final RealmConfigurationBuilder realmBuilder;

   ServerIdentitiesConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
   }

   @Override
   public void validate() {
   }

   public SSLConfigurationBuilder addSslConfiguration() {
      SSLConfigurationBuilder ssl = new SSLConfigurationBuilder(realmBuilder);
      sslConfigurations.add(ssl);
      return ssl;
   }

   @Override
   public ServerIdentitiesConfiguration create() {
      List<SSLConfiguration> sslConfigurations = this.sslConfigurations.stream()
            .map(SSLConfigurationBuilder::create).collect(Collectors.toList());
      return new ServerIdentitiesConfiguration(sslConfigurations);
   }

   @Override
   public ServerIdentitiesConfigurationBuilder read(ServerIdentitiesConfiguration template) {
      sslConfigurations.clear();
      template.sslConfigurations().forEach(s -> addSslConfiguration().read(s));
      return this;
   }
}
