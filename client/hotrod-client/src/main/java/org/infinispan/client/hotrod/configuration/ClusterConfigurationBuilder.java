package org.infinispan.client.hotrod.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;

/**
 * @since 8.1
 */
public class ClusterConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ClusterConfiguration> {

   private static final Log log = LogFactory.getLog(ClusterConfigurationBuilder.class);

   private final List<ServerConfigurationBuilder> servers = new ArrayList<ServerConfigurationBuilder>();
   private final String clusterName;

   protected ClusterConfigurationBuilder(ConfigurationBuilder builder, String clusterName) {
      super(builder);
      this.clusterName = clusterName;
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

   @Override
   public void validate() {
      if (clusterName == null || clusterName.isEmpty()) {
         throw log.missingClusterNameDefinition();
      }
      if (servers.isEmpty()) {
         throw log.missingClusterServersDefinition(clusterName);
      }
      for (ServerConfigurationBuilder serverConfigBuilder : servers) {
         serverConfigBuilder.validate();
      }
   }

   @Override
   public ClusterConfiguration create() {
      List<ServerConfiguration> serverCluster = servers.stream()
         .map(ServerConfigurationBuilder::create).collect(Collectors.toList());
      return new ClusterConfiguration(serverCluster, clusterName);
   }

   @Override
   public Builder<?> read(ClusterConfiguration template) {
      template.getCluster().forEach(server -> this.addClusterNode(server.host(), server.port()));
      return this;
   }
}
