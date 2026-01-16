package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.rest.IpFilterRule;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.NetworkAddress;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.Containers;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.testcontainers.InfinispanGenericContainer;
import org.infinispan.testing.Exceptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.Network;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class ProtocolManagementIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS =
      InfinispanServerExtensionBuilder.config("configuration/MultiEndpointClusteredServerTest.xml")
         .numServers(2)
         .property("infinispan.bind.address", "0.0.0.0")
         .build();
   private final String address1;
   private final String address2;

   public ProtocolManagementIT() {
      List<Map<String, String>> addresses = addNetworkToContainers(((ContainerInfinispanServerDriver) SERVERS.getServerDriver()).getContainers());
      Iterator<String> iterator = addresses.get(0).values().iterator();
      address1 = iterator.next();
      address2 = iterator.next();
   }

   private static List<Map<String, String>> addNetworkToContainers(List<InfinispanGenericContainer> containers) {
      Network otherNetwork = Network.newNetwork();
      List<Map<String, String>> addresses = new ArrayList<>();
      for (InfinispanGenericContainer c : containers) {
         Containers.DOCKER_CLIENT.connectToNetworkCmd().withNetworkId(otherNetwork.getId()).withContainerId(c.getContainerId()).exec();
         addresses.add(c.getNetworkIpAddresses());
      }
      return addresses;
   }

   @Test
   public void testIpFilter() {
      RestClientConfigurationBuilder address1Builder = new RestClientConfigurationBuilder();
      address1Builder.addServer().host(address1).port(11222);
      RestClient address1Client = SERVERS.rest().withClientConfiguration(address1Builder).get();
      assertStatus(200, address1Client.server().connectorNames());

      RestClientConfigurationBuilder address2Builder0 = new RestClientConfigurationBuilder();
      address2Builder0.addServer().host(address2).port(11222);
      RestClient address2Client0 = SERVERS.rest().withClientConfiguration(address2Builder0).get();
      assertStatus(200, address2Client0.server().connectorNames());

      RestClientConfigurationBuilder address2Builder1 = new RestClientConfigurationBuilder();
      address2Builder1.addServer().host(address2).port(11222);
      RestClient address2Client1 = SERVERS.rest().withClientConfiguration(address2Builder1).get();
      assertStatus(200, address2Client1.server().connectorNames());

      List<IpFilterRule> rules = new ArrayList<>();
      try {
         NetworkAddress r = NetworkAddress.inetAddress("r", address2);
         NetworkAddress remote = NetworkAddress.match("remote", iF -> Exceptions.unchecked(() -> !iF.isLoopback()), a ->
            NetworkAddress.inetAddressMatchesInterfaceAddress(r.getAddress().getAddress(), a.getAddress().getAddress(), a.getNetworkPrefixLength())
         );
         rules.add(new IpFilterRule(IpFilterRule.RuleType.REJECT, remote.cidr()));
      } catch (IOException e) {
         // Ignore unmatched
      }

      assertStatus(204, address1Client.server().connectorIpFilterSet("endpoint-default", rules));
      Exceptions.expectException(RuntimeException.class, IOException.class, () -> sync(address2Client0.server().connectorNames()));
      Exceptions.expectException(RuntimeException.class, IOException.class, () -> sync(address2Client1.server().connectorNames()));
      assertStatus(204, address1Client.server().connectorIpFiltersClear("endpoint-default"));
      assertStatus(200, address2Client0.server().connectorNames());
      assertStatus(200, address2Client1.server().connectorNames());

      // Attempt to lock ourselves out
      assertStatus(409, address2Client0.server().connectorIpFilterSet("endpoint-default", rules));

      // Apply the filter just on the Hot Rod endpoint
      assertStatus(204, address1Client.server().connectorIpFilterSet("HotRod-hotrod", rules));
      ConfigurationBuilder hotRodSiteLocalBuilder = new ConfigurationBuilder();
      hotRodSiteLocalBuilder.addServer().host(address2).port(11222).clientIntelligence(ClientIntelligence.BASIC);
      RemoteCacheManager siteLocalRemoteCacheManager = SERVERS.hotrod().withClientConfiguration(hotRodSiteLocalBuilder).createRemoteCacheManager();
      Exceptions.expectException(TransportException.class, siteLocalRemoteCacheManager::getCacheNames);
      // REST should still work, so let's clear the rules
      assertStatus(204, address2Client0.server().connectorIpFiltersClear("HotRod-hotrod"));
      // And retry
      assertNotNull(siteLocalRemoteCacheManager.getCacheNames());
   }

   @Test
   public void testConnectorStartStop() {

      RestClientConfigurationBuilder defaultBuilder = new RestClientConfigurationBuilder();
      defaultBuilder.addServer().host(address1).port(11222);
      RestClient defaultClient = SERVERS.rest().withClientConfiguration(defaultBuilder).get();
      assertStatus(200, defaultClient.caches());

      RestClientConfigurationBuilder alternateBuilder = new RestClientConfigurationBuilder();
      alternateBuilder.addServer().host(address1).port(11223);
      RestClient alternateClient = SERVERS.rest().withClientConfiguration(alternateBuilder).get();
      assertStatus(200, alternateClient.caches());

      assertStatus(204, defaultClient.server().connectorStop("endpoint-alternate-1"));
      Exceptions.expectException(RuntimeException.class, IOException.class, () -> sync(alternateClient.caches()));
      assertStatus(204, defaultClient.server().connectorStart("endpoint-alternate-1"));
      assertStatus(200, alternateClient.caches());

      // Attempt to lock ourselves out
      assertStatus(409, defaultClient.server().connectorStop("endpoint-default"));
   }
}
