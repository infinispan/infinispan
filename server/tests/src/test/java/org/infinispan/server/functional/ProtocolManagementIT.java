package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.rest.IpFilterRule;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class ProtocolManagementIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/MultiEndpointClusteredServerTest.xml")
               .runMode(ServerRunMode.EMBEDDED)
               .numServers(2)
               .property("infinispan.bind.address", "0.0.0.0")
               .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testIpFilter() throws IOException {
      NetworkAddress loopback = NetworkAddress.loopback("loopback");
      RestClientConfigurationBuilder loopbackBuilder = new RestClientConfigurationBuilder();
      loopbackBuilder.addServer().host(loopback.getAddress().getHostAddress()).port(11222);
      RestClient loopbackClient = SERVER_TEST.rest().withClientConfiguration(loopbackBuilder).get();
      assertStatus(200, loopbackClient.server().connectorNames());

      NetworkAddress siteLocal = NetworkAddress.match("sitelocal", iF -> !iF.getName().startsWith("docker"), InetAddress::isSiteLocalAddress);
      RestClientConfigurationBuilder siteLocalBuilder0 = new RestClientConfigurationBuilder();
      siteLocalBuilder0.addServer().host(siteLocal.getAddress().getHostAddress()).port(11222);
      RestClient siteLocalClient0 = SERVER_TEST.rest().withClientConfiguration(siteLocalBuilder0).get();
      assertStatus(200, siteLocalClient0.server().connectorNames());

      RestClientConfigurationBuilder siteLocalBuilder1 = new RestClientConfigurationBuilder();
      siteLocalBuilder1.addServer().host(siteLocal.getAddress().getHostAddress()).port(11322);
      RestClient siteLocalClient1 = SERVER_TEST.rest().withClientConfiguration(siteLocalBuilder1).get();
      assertStatus(200, siteLocalClient1.server().connectorNames());

      List<IpFilterRule> rules = new ArrayList<>();
      rules.add(new IpFilterRule(IpFilterRule.RuleType.REJECT, siteLocal.cidr()));

      assertStatus(204, loopbackClient.server().connectorIpFilterSet("endpoint-default", rules));
      Exceptions.expectException(RuntimeException.class, ExecutionException.class, SocketException.class, () -> sync(siteLocalClient0.server().connectorNames()));
      Exceptions.expectException(RuntimeException.class, ExecutionException.class, SocketException.class, () -> sync(siteLocalClient1.server().connectorNames()));
      assertStatus(204, loopbackClient.server().connectorIpFiltersClear("endpoint-default"));
      assertStatus(200, siteLocalClient0.server().connectorNames());
      assertStatus(200, siteLocalClient1.server().connectorNames());

      // Attempt to lock ourselves out
      assertStatus(409, siteLocalClient0.server().connectorIpFilterSet("endpoint-default", rules));

      // Apply the filter just on the Hot Rod endpoint
      assertStatus(204, loopbackClient.server().connectorIpFilterSet("HotRod-hotrod", rules));
      ConfigurationBuilder hotRodSiteLocalBuilder = new ConfigurationBuilder();
      hotRodSiteLocalBuilder.addServer().host(siteLocal.getAddress().getHostAddress()).port(11222).clientIntelligence(ClientIntelligence.BASIC);
      RemoteCacheManager siteLocalRemoteCacheManager = SERVER_TEST.hotrod().withClientConfiguration(hotRodSiteLocalBuilder).createRemoteCacheManager();
      Exceptions.expectException(TransportException.class, siteLocalRemoteCacheManager::getCacheNames);
      // REST should still work, so let's clear the rules
      assertStatus(204, siteLocalClient0.server().connectorIpFiltersClear("HotRod-hotrod"));
      // And retry
      assertNotNull(siteLocalRemoteCacheManager.getCacheNames());
   }

   @Test
   public void testConnectorStartStop() throws IOException {
      NetworkAddress loopback = NetworkAddress.loopback("loopback");
      RestClientConfigurationBuilder defaultBuilder = new RestClientConfigurationBuilder();
      defaultBuilder.addServer().host(loopback.getAddress().getHostAddress()).port(11222);
      RestClient defaultClient = SERVER_TEST.rest().withClientConfiguration(defaultBuilder).get();
      assertStatus(200, defaultClient.caches());

      RestClientConfigurationBuilder alternateBuilder = new RestClientConfigurationBuilder();
      alternateBuilder.addServer().host(loopback.getAddress().getHostAddress()).port(11223);
      RestClient alternateClient = SERVER_TEST.rest().withClientConfiguration(alternateBuilder).get();
      assertStatus(200, alternateClient.caches());

      assertStatus(204, defaultClient.server().connectorStop("endpoint-alternate-1"));
      Exceptions.expectException(RuntimeException.class, ExecutionException.class, SocketException.class, () -> sync(alternateClient.caches()));
      assertStatus(204, defaultClient.server().connectorStart("endpoint-alternate-1"));
      assertStatus(200, alternateClient.caches());

      // Attempt to lock ourselves out
      assertStatus(409, defaultClient.server().connectorStop("endpoint-default"));
   }
}
