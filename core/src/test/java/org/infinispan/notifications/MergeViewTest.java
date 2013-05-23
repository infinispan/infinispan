/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.notifications;

import org.infinispan.config.Configuration;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

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
      addClusterEnabledCacheManager(Configuration.CacheMode.REPL_SYNC, true,
                                    new TransportFlags().withMerge(true));

      ml0 = new MergeListener();
      manager(0).addListener(ml0);

      discard = TestingUtil.getDiscardForCache(cache(0));
      discard.setDiscardAll(true);

      addClusterEnabledCacheManager(Configuration.CacheMode.REPL_SYNC, true,
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
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

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
