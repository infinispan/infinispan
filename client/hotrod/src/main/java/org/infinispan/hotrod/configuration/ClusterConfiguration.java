package org.infinispan.hotrod.configuration;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 14.0
 */
public class ClusterConfiguration extends ConfigurationElement<ClusterConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   // default intelligence is "null" to use the client intelligence defined globally
   static final AttributeDefinition<ClientIntelligence> CLIENT_INTELLIGENCE = AttributeDefinition.builder("client_intelligence", null, ClientIntelligence.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusterConfiguration.class, NAME, CLIENT_INTELLIGENCE);
   }

   private final List<ServerConfiguration> servers;

   ClusterConfiguration(AttributeSet attributes, List<ServerConfiguration> servers) {
      super("cluster", attributes);
      this.servers = servers;
   }

   public List<ServerConfiguration> getServers() {
      return servers;
   }

   public String getClusterName() {
      return attributes.attribute(NAME).get();
   }

   public ClientIntelligence getClientIntelligence() {
      return attributes.attribute(CLIENT_INTELLIGENCE).get();
   }
}
