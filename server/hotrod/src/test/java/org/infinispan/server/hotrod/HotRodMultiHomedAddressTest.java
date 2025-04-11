package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
@Test(testName = "server.hotrod.HotRodMultiHomedAddressTest", groups = "functional")
public class HotRodMultiHomedAddressTest extends HotRodMultiNodeTest {

   public void testInAddrAny() throws IOException {
      // GitHub actions don't support IPv6 https://github.com/actions/runner-images/issues/668
      SkipTestNG.skipProperty("github.action");
      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements(); ) {
         NetworkInterface netif = en.nextElement();
         for (InterfaceAddress address : netif.getInterfaceAddresses()) {
            HotRodClient hotRodClient = createClient(servers().get(0), cacheName(), address.getAddress().getHostAddress());
            TestResponse ping = hotRodClient.ping((byte) 2, 0);
            assertEquals(2, ping.topologyResponse.members.size());
            for(ServerAddress serverAddress : ping.topologyResponse.members) {
               InetAddress inetAddress = InetAddress.getByName(serverAddress.getHost(null));
               assertTrue(MultiHomedServerAddress.inetAddressMatchesInterfaceAddress(inetAddress.getAddress(), address.getAddress().getAddress(), address.getNetworkPrefixLength()));
            }
            Exceptions.unchecked(() -> hotRodClient.stop().await());
         }
      }
   }

   @Override
   protected String cacheName() {
      return "multi";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   @Override
   protected HotRodServer startTestHotRodServer(EmbeddedCacheManager cacheManager, int port) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyNetworkPrefixOverride(false);
      return startHotRodServer(cacheManager, "0.0.0.0", port, builder);
   }
}
