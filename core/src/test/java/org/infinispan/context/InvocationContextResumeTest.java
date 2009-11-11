/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat, Inc. and/or its affiliates, and
 * individual contributors as indicated by the @author tags. See the
 * copyright.txt file in the distribution for a full listing of
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
package org.infinispan.context;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional"}, testName = "marshall.InvocationContextInterceptorErrorTest")
public class InvocationContextResumeTest extends MultipleCacheManagersTest {
   private static final Log log = LogFactory.getLog(InvocationContextResumeTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = TestCacheManagerFactory.getDefaultConfiguration(true);
      cfg.setSyncCommitPhase(true);
      cfg.setSyncRollbackPhase(true);
      createClusteredCaches(1, "timestamps", cfg);
   }

   public void testMishavingListenerResumesContext() {
      Cache cache = cache(0, "timestamps");
      cache.addListener(new CacheListener());
      try {
         cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).put("k", "v");
      } catch (CacheException ce) {
         assert ce.getCause() instanceof NullPointerException;
      }
   }

   @Listener
   public static class CacheListener {
      @CacheEntryModified
      public void entryModified(CacheEntryModifiedEvent event) {
         if (!event.isPre()) {
            log.debug("Entry modified: {0}, let's throw an NPE!!", event);
            throw new NullPointerException();
         }
      }
   }
}
