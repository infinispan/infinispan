package org.infinispan.server.test;

import static org.infinispan.server.test.ContainerUtil.getIpAddressFromContainer;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.testcontainers.containers.GenericContainer;

public class ContainerRemoteCacheManager {

   private final List<GenericContainer> containers;

   public ContainerRemoteCacheManager(List<GenericContainer> containers) {
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
                     GenericContainer container = getGeneriContainerBy(inetSocketAddress);
                     localHostServers.add(new InetSocketAddress("localhost", container.getMappedPort(inetSocketAddress.getPort())));
                  }
                  return super.updateTopologyInfo(cacheName, localHostServers, quiet);
               }
            };
         }
      };
   }

   private GenericContainer getGeneriContainerBy(InetSocketAddress inetSocketAddress) {
      for (GenericContainer container : containers) {
         String hostName = getIpAddressFromContainer(container);
         if (inetSocketAddress.getHostName().equals(hostName)) {
            return container;
         }
      }
      throw new IllegalStateException();
   }
}
