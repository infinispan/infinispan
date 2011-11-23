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

package org.infinispan.notifications.cachelistener;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.api.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Tests the behaviour of caches when hooked listeners
 * throw exceptions under different circumstances.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.ListenerExceptionTest")
public class ListenerExceptionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = getDefaultClusteredConfig(
            Configuration.CacheMode.REPL_SYNC, true);
      createClusteredCaches(2, cfg);
   }

   public void testPreOpExceptionListenerOnCreate(Method m) {
      doCallsWithExcepList(m, true, FailureLocation.ON_CREATE);
   }

   public void testPostOpExceptionListenerOnCreate(Method m) {
      doCallsWithExcepList(m, false, FailureLocation.ON_CREATE);
   }

   public void testPreOpExceptionListenerOnPut(Method m) {
      manager(0).getCache().put(k(m), "init");
      doCallsWithExcepList(m, true, FailureLocation.ON_MODIFIED);
   }

   public void testPostOpExceptionListenerOnPut(Method m) {
      manager(0).getCache().put(k(m), "init");
      doCallsWithExcepList(m, false, FailureLocation.ON_MODIFIED);
   }

   private void doCallsWithExcepList(Method m, boolean isInjectInPre,
                                     FailureLocation failLoc) {
      Cache<String, String> cache = manager(0).getCache();
      ErrorInducingListener listener =
            new ErrorInducingListener(isInjectInPre, failLoc);
      cache.addListener(listener);
      try {
         cache.put(k(m), v(m));
      } catch (CacheException e) {
         assert e.getCause() instanceof SuspectException;
         // Expected, now try to simulate a failover
         listener.injectFailure = false;
         manager(1).getCache().put(k(m), v(m, 2));
         return;
      }
      fail("Should have failed");
   }

   @Listener
   public static class ErrorInducingListener {
      boolean injectFailure = true;
      boolean isInjectInPre;
      FailureLocation failureLocation;

      public ErrorInducingListener(boolean injectInPre, FailureLocation failLoc) {
         this.isInjectInPre = injectInPre;
         this.failureLocation = failLoc;
      }

      @CacheEntryCreated
      public void entryCreated(CacheEntryEvent event) throws Exception {
         if (failureLocation == FailureLocation.ON_CREATE)
            injectFailure(event);
      }

      @CacheEntryModified
      public void entryModified(CacheEntryEvent event) throws Exception {
         if (failureLocation == FailureLocation.ON_MODIFIED)
            injectFailure(event);
      }

      private void injectFailure(CacheEntryEvent event) {
         if (injectFailure) {
            if (isInjectInPre && event.isPre())
               throwSuspectException();
            else if (!isInjectInPre && !event.isPre())
               throwSuspectException();
         }
      }

      private void throwSuspectException() {
         throw new SuspectException(String.format(
            "Simulated suspicion when isPre=%b and in %s",
            isInjectInPre, failureLocation));
      }

   }

   private static enum FailureLocation {
      ON_CREATE, ON_MODIFIED
   }

}
