package org.infinispan.client.hotrod.xsite;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Optional;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.testng.annotations.Test;

@Test(groups = "unstable", testName = "client.hotrod.xsite.SiteManualSwitchTest",
   description = "Disabled for random failure, tracked by ISPN-9064")
public class SiteManualSwitchTest extends AbstractHotRodSiteFailoverTest {

   @Override
   protected void createSites() {
      super.createSites();
      addHitCountInterceptors();
   }

   public void testManualClusterSwitch() {
      RemoteCacheManager clientA = client(SITE_A, Optional.of(SITE_B));
      RemoteCacheManager clientB = client(SITE_B, Optional.empty());

      RemoteCache<Integer, String> cacheA = clientA.getCache();
      RemoteCache<Integer, String> cacheB = clientB.getCache();

      assertNoHits();

      assertSingleSiteHit(SITE_A, SITE_B, () -> assertNull(cacheA.put(1, "v1")));
      assertSingleSiteHit(SITE_A, SITE_B, () -> assertEquals("v1", cacheA.get(1)));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertNull(cacheB.put(2, "v2")));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertEquals("v2", cacheB.get(2)));

      assertTrue(clientA.switchToCluster(SITE_B));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertNull(cacheA.put(3, "v3")));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertEquals("v3", cacheA.get(3)));

      assertTrue(clientA.switchToDefaultCluster());
      assertSingleSiteHit(SITE_A, SITE_B, () -> assertNull(cacheA.put(4, "v4")));
      assertSingleSiteHit(SITE_A, SITE_B, () -> assertEquals("v4", cacheA.get(4)));
   }

   private void assertSingleSiteHit(String siteHit, String siteNotHit, Runnable r) {
      r.run();
      assertSiteHit(siteHit, 1);
      assertSiteNotHit(siteNotHit);
   }

}
