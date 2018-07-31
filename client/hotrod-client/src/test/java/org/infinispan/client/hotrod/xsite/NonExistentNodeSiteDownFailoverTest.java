package org.infinispan.client.hotrod.xsite;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

@Test(groups = "functional", testName = "client.hotrod.xsite.NonExistentNodeSiteDownFailoverTest")
public class NonExistentNodeSiteDownFailoverTest extends AbstractHotRodSiteFailoverTest {

   public void testRotateSiteDownFailover() {
      RemoteCacheManager clientA = client(SITE_A, Optional.of(SITE_B));
      RemoteCache<Integer, String> cacheA = clientA.getCache();

      assertNull(cacheA.put(1, "v1"));
      assertEquals("v1", cacheA.get(1));

      int portServerSiteA = findServerPort(SITE_A);
      killSite(SITE_A);

      // Client should reconnect to surviving site
      cacheA.get(1);

      // Restart previously shut down site
      createHotRodSite(SITE_A, SITE_B, Optional.of(portServerSiteA));

      // Client still connected with surviving site
      cacheA.get(1);

      int portServerSiteB = findServerPort(SITE_B);
      killSite(SITE_B);

      // Client will reconnect to original site
      cacheA.get(1);

      // Restart previously shut down site and shutdown main site
      createHotRodSite(SITE_B, SITE_A, Optional.of(portServerSiteB));
      killSite(SITE_A);

      // Client connected with surviving site
      cacheA.get(1);
   }

   RemoteCacheManager client(String siteName, Optional<String> backupSiteName) {
      HotRodServer server = siteServers.get(siteName).get(0);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
         new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
         .addServer().host("localhost").port(1234) // Non-existent node
         .addServer().host("localhost").port(server.getPort())
         .maxRetries(3); // Some retries so that shutdown nodes can be skipped

      Optional<Integer> backupPort = backupSiteName.map(name -> {
         HotRodServer backupServer = siteServers.get(name).get(0);
         clientBuilder.addCluster(name).addClusterNode("localhost", backupServer.getPort());
         return backupServer.getPort();
      });

      if (backupPort.isPresent())
         log.debugf("Client for site '%s' connecting to main server in port %d, and backup cluster node port is %d",
            siteName, server.getPort(), backupPort.get());
      else
         log.debugf("Client for site '%s' connecting to main server in port %d",
            siteName, server.getPort());

      return new InternalRemoteCacheManager(clientBuilder.build());
   }


}
