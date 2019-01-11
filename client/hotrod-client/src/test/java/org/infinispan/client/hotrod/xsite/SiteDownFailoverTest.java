package org.infinispan.client.hotrod.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Optional;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.xsite.SiteDownFailoverTest")
public class SiteDownFailoverTest extends AbstractHotRodSiteFailoverTest {

   private RemoteCacheManager clientA;
   private RemoteCacheManager clientB;

   public void testFailoverAfterSiteShutdown() {
      clientA = client(SITE_A, Optional.of(SITE_B));
      clientB = client(SITE_B, Optional.empty());
      RemoteCache<Integer, String> cacheA = clientA.getCache();
      RemoteCache<Integer, String> cacheB = clientB.getCache();

      assertNull(cacheA.put(1, "v1"));
      assertEquals("v1", cacheA.get(1));
      assertEquals("v1", cacheB.get(1));

      int portServerSiteA = findServerPort(SITE_A);
      killSite(SITE_A);

      // Client connected with surviving site should find data
      assertEquals("v1", cacheB.get(1));

      // Client connected to crashed site should failover
      assertEquals("v1", cacheA.get(1));

      // Restart previously shut down site
      createHotRodSite(SITE_A, SITE_B, Optional.of(portServerSiteA));

      killSite(SITE_B);

      // Client that had details for site A should failover back
      // There is no data in original site since state transfer is not enabled
      assertNull(cacheA.get(1));
      assertNull(cacheA.put(2, "v2"));
      assertEquals("v2", cacheA.get(2));
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      HotRodClientTestingUtil.killRemoteCacheManagers(clientA, clientB);

      super.destroy();
   }
}
