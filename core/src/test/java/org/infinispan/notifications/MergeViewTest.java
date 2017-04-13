package org.infinispan.notifications;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "notifications.MergeViewTest")
public class MergeViewTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(MergeViewTest.class);

   private DISCARD discard;
   private MergeListener ml0;
   private MergeListener ml1;

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true),
                                    new TransportFlags().withMerge(true));

      ml0 = new MergeListener();
      manager(0).addListener(ml0);

      discard = TestingUtil.getDiscardForCache(cache(0));
      discard.setDiscardAll(true);

      addClusterEnabledCacheManager(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true),
                                    new TransportFlags().withMerge(true));
      ml1 = new MergeListener();
      manager(1).addListener(ml1);

      cache(0).put("k", "v0");
      cache(1).put("k", "v1");
      Thread.sleep(2000);


      assert advancedCache(0).getRpcManager().getTransport().getMembers().size() == 1;
      assert advancedCache(1).getRpcManager().getTransport().getMembers().size() == 1;
   }

   public void testMergeViewHappens() {
      discard.setDiscardAll(false);
      TestingUtil.blockUntilViewsReceived(60000, cache(0), cache(1));
      TestingUtil.waitForStableTopology(cache(0), cache(1));

      assert ml0.isMerged && ml1.isMerged;

      cache(0).put("k", "v2");
      assertEquals(cache(0).get("k"), "v2");
      assertEquals(cache(1).get("k"), "v2");
   }

   @Listener
   public static class MergeListener {
      volatile boolean isMerged;

      @Merged
      public void viewMerged(MergeEvent vce) {
         log.info("vce = " + vce);
         isMerged = true;
      }
   }
}
