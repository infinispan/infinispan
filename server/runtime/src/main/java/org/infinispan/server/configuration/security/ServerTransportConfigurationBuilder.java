package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.ServerTransportConfiguration.DATA_SOURCE;
import static org.infinispan.server.configuration.security.ServerTransportConfiguration.SECURITY_REALM;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class ServerTransportConfigurationBuilder implements Builder<ServerTransportConfiguration> {
   private final AttributeSet attributes;

   public ServerTransportConfigurationBuilder() {
      this.attributes = ServerTransportConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ServerTransportConfiguration create() {
      return new ServerTransportConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(ServerTransportConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   public ServerTransportConfigurationBuilder securityRealm(String securityRealm) {
      attributes.attribute(SECURITY_REALM).set(securityRealm);
      return this;
   }

   public ServerTransportConfigurationBuilder dataSource(String dataSource) {
      attributes.attribute(DATA_SOURCE).set(dataSource);
      return this;
   }
}
