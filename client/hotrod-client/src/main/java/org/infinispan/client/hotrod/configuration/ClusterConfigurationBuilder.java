package org.infinispan.client.hotrod.configuration;

import org.infinispan.commons.configuration.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @since 8.1
 */
public class ClusterConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ClusterConfiguration> {

   private final List<ServerConfigurationBuilder> servers = new ArrayList<ServerConfigurationBuilder>();
   private final String clusterName;

   protected ClusterConfigurationBuilder(ConfigurationBuilder builder, String clusterName) {
      super(builder);
      this.clusterName = clusterName;
   }

   public ClusterConfigurationBuilder addClusterNode(String host, int port) {
      ServerConfigurationBuilder serverBuilder = new ServerConfigurationBuilder(builder);
      servers.add(serverBuilder.host(host).port(port));
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ClusterConfiguration create() {
      List<ServerConfiguration> serverCluster = servers.stream()
         .map(ServerConfigurationBuilder::create).collect(Collectors.toList());
      return new ClusterConfiguration(serverCluster, clusterName);
   }

   @Override
   public Builder<?> read(ClusterConfiguration template) {
      template.getCluster().stream()
         .forEach(server -> this.addClusterNode(server.host(), server.port()));
      return this;
   }
}
