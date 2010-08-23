package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional" , testName = "client.hotrod.DistTopologyChangeTest")
public class DistTopologyChangeTest extends ReplTopologyChangeTest {
   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.DIST_SYNC;
   }

   @Override
   protected void waitForClusterToForm(int memberCount) {
      super.waitForClusterToForm(memberCount);
      List<Cache> caches = new ArrayList<Cache>();
      for (int i = 0; i < memberCount; i++) {
         caches.add(manager(i).getCache());
      }
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(caches);
   }
}
