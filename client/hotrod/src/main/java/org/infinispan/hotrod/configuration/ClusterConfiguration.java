package org.infinispan.hotrod.configuration;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 14.0
 */
public class ClusterConfiguration extends ConfigurationElement<ClusterConfiguration> {
   public static final String DEFAULT_CLUSTER_NAME = "___DEFAULT-CLUSTER___";
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusterConfiguration.class, NAME);
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
}
