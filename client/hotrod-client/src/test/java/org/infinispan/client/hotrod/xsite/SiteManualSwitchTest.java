package org.infinispan.client.hotrod.xsite;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.remoteCacheManagerObjectName;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Optional;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.xsite.SiteManualSwitchTest")
public class SiteManualSwitchTest extends AbstractHotRodSiteFailoverTest {
   RemoteCacheManager clientA;
   RemoteCacheManager clientB;

   @Override
   protected void createSites() {
      super.createSites();
      addHitCountInterceptors();
   }

   @AfterMethod(alwaysRun = true)
   public void killClients() {
      HotRodClientTestingUtil.killRemoteCacheManagers(clientA, clientB);
   }

   public void testManualClusterSwitch() {
      clientA = client(SITE_A, Optional.of(SITE_B));
      clientB = client(SITE_B, Optional.empty());

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

   @Test(enabled = false)
   public void testManualClusterSwitchViaJMX() throws Exception {
      clientA = client(SITE_A, Optional.of(SITE_B));
      clientB = client(SITE_B, Optional.empty());

      MBeanServer mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName objectName = remoteCacheManagerObjectName(clientA);

      RemoteCache<Integer, String> cacheA = clientA.getCache();
      RemoteCache<Integer, String> cacheB = clientB.getCache();

      assertNoHits();

      assertSingleSiteHit(SITE_A, SITE_B, () -> assertNull(cacheA.put(1, "v1")));
      assertSingleSiteHit(SITE_A, SITE_B, () -> assertEquals("v1", cacheA.get(1)));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertNull(cacheB.put(2, "v2")));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertEquals("v2", cacheB.get(2)));

      Object switched = mbeanServer.invoke(objectName, "switchToCluster", new Object[]{SITE_B}, new String[]{String.class.getName()});
      assertEquals(Boolean.TRUE, switched);
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertNull(cacheA.put(3, "v3")));
      assertSingleSiteHit(SITE_B, SITE_A, () -> assertEquals("v3", cacheA.get(3)));

      switched = mbeanServer.invoke(objectName, "switchToDefaultCluster", new Object[]{}, new String[]{});
      assertEquals(Boolean.TRUE, switched);
      assertSingleSiteHit(SITE_A, SITE_B, () -> assertNull(cacheA.put(4, "v4")));
      assertSingleSiteHit(SITE_A, SITE_B, () -> assertEquals("v4", cacheA.get(4)));
   }

   private void assertSingleSiteHit(String siteHit, String siteNotHit, Runnable r) {
      r.run();
      assertSiteHit(siteHit, 1);
      assertSiteNotHit(siteNotHit);
      resetHitCounters();
   }

}
