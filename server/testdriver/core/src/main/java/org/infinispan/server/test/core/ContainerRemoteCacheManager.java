package org.infinispan.server.test.core;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

public class ContainerRemoteCacheManager {

   private final InfinispanGenericContainer[] containers;

   public ContainerRemoteCacheManager(InfinispanGenericContainer[] containers) {
      this.containers = containers;
   }

   public RemoteCacheManager wrap(ConfigurationBuilder builder) {
      return new RemoteCacheManager(builder.build()) {
         @Override
         public ChannelFactory createChannelFactory() {
            return new ChannelFactory() {
               protected Collection<SocketAddress> updateTopologyInfo(byte[] cacheName, Collection<SocketAddress> newServers, boolean quiet) {
                  List<SocketAddress> localHostServers = new ArrayList<>();
                  for (SocketAddress address : newServers) {
                     InetSocketAddress inetSocketAddress = (InetSocketAddress) address;
                     InfinispanGenericContainer container = getGeneriContainerBy(inetSocketAddress);
                     localHostServers.add(new InetSocketAddress("localhost", container.getMappedPort(inetSocketAddress.getPort())));
                  }
                  return super.updateTopologyInfo(cacheName, localHostServers, quiet);
               }
            };
         }
      };
   }

   private InfinispanGenericContainer getGeneriContainerBy(InetSocketAddress inetSocketAddress) {
      for (InfinispanGenericContainer container : containers) {
         String hostName = container.getNetworkIpAddress();
         if (inetSocketAddress.getHostName().equals(hostName)) {
            return container;
         }
      }
      throw new IllegalStateException();
   }
}
