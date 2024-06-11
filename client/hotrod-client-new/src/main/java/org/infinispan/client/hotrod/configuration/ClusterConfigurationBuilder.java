package org.infinispan.client.hotrod.configuration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 8.1
 */
public class ClusterConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ClusterConfiguration> {

   private final List<ServerConfigurationBuilder> servers = new ArrayList<>();
   private final String clusterName;
   private ClientIntelligence intelligence;
   private String sniHostName;

   protected ClusterConfigurationBuilder(ConfigurationBuilder builder, String clusterName) {
      super(builder);
      this.clusterName = clusterName;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   public String getClusterName() {
      return clusterName;
   }

   public ClusterConfigurationBuilder addClusterNode(String host, int port) {
      ServerConfigurationBuilder serverBuilder = new ServerConfigurationBuilder(builder);
      servers.add(serverBuilder.host(host).port(port));
      return this;
   }

   public ClusterConfigurationBuilder addClusterNodes(String serverList) {
      ConfigurationBuilder.parseServers(serverList, (host, port) -> {
         ServerConfigurationBuilder serverBuilder = new ServerConfigurationBuilder(builder);
         servers.add(serverBuilder.host(host).port(port));
      });
      return this;
   }

   public ClusterConfigurationBuilder clusterClientIntelligence(ClientIntelligence intelligence) {
      // null is valid, means using the global intelligence (for backwards compatibility)
      this.intelligence = intelligence;
      return this;
   }

   public ClusterConfigurationBuilder clusterSniHostName(String clusterSniHostName) {
      this.sniHostName = clusterSniHostName;
      return this;
   }

   @Override
   public void validate() {
      if (clusterName == null || clusterName.isEmpty()) {
         throw HOTROD.missingClusterNameDefinition();
      }
      if (servers.isEmpty()) {
         throw HOTROD.missingClusterServersDefinition(clusterName);
      }
      for (ServerConfigurationBuilder serverConfigBuilder : servers) {
         serverConfigBuilder.validate();
      }
   }

   @Override
   public ClusterConfiguration create() {
      List<ServerConfiguration> serverCluster = servers.stream()
         .map(ServerConfigurationBuilder::create).collect(Collectors.toList());
      return new ClusterConfiguration(serverCluster, clusterName, intelligence, sniHostName);
   }

   @Override
   public Builder<?> read(ClusterConfiguration template, Combine combine) {
      template.getCluster().forEach(server -> this.addClusterNode(server.host(), server.port()));
      clusterClientIntelligence(template.getClientIntelligence());
      return this;
   }
}
