package org.infinispan.hotrod.configuration;

import static org.infinispan.hotrod.configuration.ClusterConfiguration.CLIENT_INTELLIGENCE;
import static org.infinispan.hotrod.configuration.ClusterConfiguration.NAME;
import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 14.0
 */
public class ClusterConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ClusterConfiguration> {
   private final List<ServerConfigurationBuilder> servers = new ArrayList<>();
   private final AttributeSet attributes = ClusterConfiguration.attributeDefinitionSet();

   protected ClusterConfigurationBuilder(HotRodConfigurationBuilder builder, String clusterName) {
      super(builder);
      attributes.attribute(NAME).set(clusterName);
   }

   public String getClusterName() {
      return attributes.attribute(NAME).get();
   }

   public ClusterConfigurationBuilder addClusterNode(String host, int port) {
      ServerConfigurationBuilder serverBuilder = new ServerConfigurationBuilder(builder);
      servers.add(serverBuilder.host(host).port(port));
      return this;
   }

   public ClusterConfigurationBuilder addClusterNodes(String serverList) {
      HotRodConfigurationBuilder.parseServers(serverList, (host, port) -> {
         ServerConfigurationBuilder serverBuilder = new ServerConfigurationBuilder(builder);
         servers.add(serverBuilder.host(host).port(port));
      });
      return this;
   }

   public ClusterConfigurationBuilder clusterClientIntelligence(ClientIntelligence intelligence) {
      // null is valid, means using the global intelligence (for backwards compatibility)
      attributes.attribute(CLIENT_INTELLIGENCE).set(intelligence);
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(NAME).isNull()) {
         throw HOTROD.missingClusterNameDefinition();
      }
      if (servers.isEmpty()) {
         throw HOTROD.missingClusterServersDefinition(getClusterName());
      }
      for (ServerConfigurationBuilder serverConfigBuilder : servers) {
         serverConfigBuilder.validate();
      }
   }

   @Override
   public ClusterConfiguration create() {
      List<ServerConfiguration> serverCluster = servers.stream()
         .map(ServerConfigurationBuilder::create).collect(Collectors.toList());
      return new ClusterConfiguration(attributes, serverCluster);
   }

   @Override
   public Builder<?> read(ClusterConfiguration template) {
      template.getServers().forEach(server -> this.addClusterNode(server.host(), server.port()));
      return this;
   }
}
