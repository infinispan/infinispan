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

package org.infinispan.invalidation;

import org.infinispan.AdvancedCache;
import org.infinispan.config.Configuration;
import org.infinispan.marshall.NotSerializableException;
import org.infinispan.replication.ReplicationExceptionTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNotNull;

/**
 * Test to verify how the invalidation works under exceptional circumstances.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "invalidation.InvalidationExceptionTest")
public class InvalidationExceptionTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration invalidAsync = getDefaultClusteredConfig(
            Configuration.CacheMode.INVALIDATION_ASYNC, false);
      createClusteredCaches(2, "invalidAsync", invalidAsync);

      Configuration replQueue = getDefaultClusteredConfig(
            Configuration.CacheMode.INVALIDATION_ASYNC, false);
      replQueue.setUseReplQueue(true);
      defineConfigurationOnAllManagers("invalidReplQueueCache", replQueue);

      Configuration asyncMarshall = getDefaultClusteredConfig(
            Configuration.CacheMode.INVALIDATION_ASYNC, false);
      asyncMarshall.setUseAsyncMarshalling(true);
      defineConfigurationOnAllManagers("invalidAsyncMarshallCache", asyncMarshall);
   }

   public void testNonSerializableAsyncInvalid() throws Exception {
      doNonSerializableInvalidTest("invalidAsync");
   }

   public void testNonSerializableReplQueue() throws Exception {
      doNonSerializableInvalidTest("invalidReplQueueCache");
   }

   public void testNonSerializableAsyncMarshalling() throws Exception {
      doNonSerializableInvalidTest("invalidAsyncMarshallCache");
   }

   private void doNonSerializableInvalidTest(String cacheName) {
      AdvancedCache cache1 = cache(0, cacheName).getAdvancedCache();
      AdvancedCache cache2 = cache(1, cacheName).getAdvancedCache();
      try {
         cache1.put(new ReplicationExceptionTest.ContainerData(), "test-" + cacheName);
         // We should not come here.
         assertNotNull("NonSerializableData should not be null on cache2", cache2.get("test"));
      } catch (RuntimeException runtime) {
         Throwable t = runtime.getCause();
         if (runtime instanceof NotSerializableException
                  || t instanceof NotSerializableException
                  || t.getCause() instanceof NotSerializableException) {
            log.trace("received NotSerializableException - as expected");
         } else {
            throw runtime;
         }
      }
   }

}
