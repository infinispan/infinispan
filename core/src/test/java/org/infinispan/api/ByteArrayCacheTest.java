/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayEquivalence;
import org.infinispan.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;


/**
 * Test that verifies that when custom, or JDK, objects that have undesirable
 * equality checks, i.e. byte arrays, are stored in the cache, then the
 * correct results are returned with different configurations (with or
 * without key/value equivalence set up).
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "api.ByteArrayCacheTest")
@CleanupAfterMethod
public class ByteArrayCacheTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // If key equivalence is set, it will also be used for value
      builder.dataContainer().keyEquivalence(ByteArrayEquivalence.INSTANCE);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testByteArrayReplaceFailWithoutEquivalence() {
      final Integer key = 1;
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createLocalCacheManager(false)) {
         @Override
         public void call() {
            Cache<Integer, byte[]> cache = cm.getCache();
            final byte[] value = {1, 2, 3};
            cache.put(key, value);
            // Use a different instance deliberately
            final byte[] oldValue = {1, 2, 3};
            final byte[] newValue = {4, 5, 6};
            assertFalse(cache.replace(key, oldValue, newValue));
         }
      });
   }

   public void testByteArrayValueOnlyReplace() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer().valueEquivalence(ByteArrayEquivalence.INSTANCE);
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(builder)) {
         @Override
         public void call() {
            // Mimics Memcached/REST endpoints where only value side is byte array
            Cache<Integer, byte[]> cache = cm.getCache();
            final Integer key = 2;
            final byte[] value = {1, 2, 3};
            cache.put(key, value);
            // Use a different instance deliberately
            final byte[] oldValue = {1, 2, 3};
            final byte[] newValue = {4, 5, 6};
            assertTrue(cache.replace(key, oldValue, newValue));
         }
      });
   }

   public void testByteArrayGet() {
      byteArrayGet(this.<byte[], byte[]>cache(), true);
   }

   public void testByteArrayGetFail() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createLocalCacheManager(false)) {
         @Override
         public void call() {
            byteArrayGet(cm.<byte[], byte[]>getCache(), false);
         }
      });
   }

   protected void byteArrayGet(
         Map<byte[], byte[]> map, boolean expectFound) {
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      map.put(key, value);
      byte[] lookupKey = {1, 2, 3}; // on purpose, different instance required
      if (expectFound)
         assertTrue(String.format("Expected key=%s to return value=%s",
               Util.toStr(lookupKey), Util.toStr(value)),
               Arrays.equals(value, map.get(lookupKey)));
      else
         assertNull(map.get(lookupKey));
   }

}
