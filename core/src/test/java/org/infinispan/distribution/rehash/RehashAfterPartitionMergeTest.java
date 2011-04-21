/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.rehash;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.test.TestingUtil;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName =  "distribution.rehash.RehashAfterPartitionMergeTest", enabled = true, description = "Need to revisit after https://jira.jboss.org/browse/ISPN-493")
public class RehashAfterPartitionMergeTest extends BaseDistFunctionalTest {
   
   DISCARD d1, d2;

   
   public RehashAfterPartitionMergeTest() {
      super();
      this.INIT_CLUSTER_SIZE = 2;
      this.sync = true;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      d1 = TestingUtil.getDiscardForCache(c1);
      d2 = TestingUtil.getDiscardForCache(c2);
   }

   public void testCachePartition() {
      c1.put("1", "value");
      c2.put("2", "value");

      for (Cache<Object, String> c: caches) {
         assert "value".equals(c.get("1"));
         assert "value".equals(c.get("2"));
         assert manager(c).getMembers().size() == 2;
      }
      
      AtomicInteger ai = new AtomicInteger(0);
      manager(c1).addListener(new ViewChangeListener(ai));
      manager(c2).addListener(new ViewChangeListener(ai));

      d1.setDiscardAll(true);
      d2.setDiscardAll(true);

      // Wait till *both* instances have seen the view change.
      while (ai.get() < 2) TestingUtil.sleepThread(500);
      
      
      
      // we should see a network partition
      for (Cache<Object, String> c: caches) assert manager(c).getMembers().size() == 1;

      c1.put("3", "value");
      c2.put("4", "value");

      assert "value".equals(c1.get("3"));
      assert null == c2.get("3");

      assert "value".equals(c2.get("4"));
      assert null == c1.get("4");

      ai.set(0);

      // lets "heal" the partition
      d1.setDiscardAll(false);
      d2.setDiscardAll(false);

      // wait till we see the view change
      while (ai.get() < 2) TestingUtil.sleepThread(500);

      c1.put("5", "value");
      c2.put("6", "value");
      for (Cache<Object, String> c: caches) {
         assert "value".equals(c.get("5"));
         assert "value".equals(c.get("6"));
         assert manager(c).getMembers().size() == 2;
      }      
   }

   @Listener
   public static class ViewChangeListener {
      AtomicInteger ai;

      private ViewChangeListener(AtomicInteger ai) {
         this.ai = ai;
      }

      @ViewChanged
      public void handle(ViewChangedEvent e) {
         ai.getAndIncrement();
      }
   }
}
