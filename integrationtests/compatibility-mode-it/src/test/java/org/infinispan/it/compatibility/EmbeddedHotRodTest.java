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

package org.infinispan.it.compatibility;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * Test compatibility between embedded caches and Hot Rod endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.CompatibilityTest")
public class EmbeddedHotRodTest extends AbstractInfinispanTest {

   CompatibilityCacheFactory<Integer, String> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<Integer, String>();
      cacheFactory.setup();
   }

   @AfterClass
   protected void teardown() {
      cacheFactory.teardown();
   }

   public void testEmbeddedPutHotRodGet() {
      final Integer key = 1;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.put(key, "v2"));
      assertEquals("v2", remote.get(key));
   }

   public void testHotRodPutEmbeddedGet() {
      final Integer key = 2;
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", embedded.get(key));
      assertEquals(null, remote.put(key, "v2"));
      assertEquals("v2", remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v3"));
      assertEquals("v3", embedded.get(key));
   }

   public void testEmbeddedPutIfAbsentHotRodGet() {
      final Integer key = 3;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.putIfAbsent(key, "v1"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.putIfAbsent(key, "v2"));
      assertEquals("v1", remote.get(key));
   }

   public void testHotRodPutIfAbsentEmbeddedGet() {
      final Integer key = 4;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, "v1"));
      assertEquals("v1", embedded.get(key));
      assertEquals(null, remote.putIfAbsent(key, "v2"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, "v2"));
      assertEquals("v1", embedded.get(key));
   }

   public void testEmbeddedReplaceHotRodGet() {
      final Integer key = 5;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.replace(key, "v1"));
      assertEquals(null, embedded.put(key, "v1"));
      assertEquals("v1", embedded.replace(key, "v2"));
      assertEquals("v2", remote.get(key));
   }

   public void testHotRodReplaceEmbeddedGet() {
      final Integer key = 6;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).replace(key, "v1"));
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).replace(key, "v2"));
      assertEquals("v2", embedded.get(key));
   }

   public void testEmbeddedReplaceConditionalHotRodGet() {
      final Integer key = 7;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertTrue(embedded.replace(key, "v1", "v2"));
      assertEquals("v2", remote.get(key));
   }

   public void testHotRodReplaceConditionalEmbeddedGet() {
      final Integer key = 8;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.put(key, "v1"));
      VersionedValue<String> versioned = remote.getVersioned(key);
      assertEquals("v1", versioned.getValue());
      assertTrue(0 != versioned.getVersion());
      assertFalse(remote.replaceWithVersion(key, "v2", Long.MAX_VALUE));
      assertTrue(remote.replaceWithVersion(key, "v2", versioned.getVersion()));
      assertEquals("v2", embedded.get(key));
   }

   public void testEmbeddedRemoveHotRodGet() {
      final Integer key = 9;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertEquals("v1", embedded.remove(key));
      assertEquals(null, remote.get(key));
   }

   public void testHotRodRemoveEmbeddedGet() {
      final Integer key = 10;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
      assertEquals(null, embedded.get(key));
   }

   public void testEmbeddedRemoveConditionalHotRodGet() {
      final Integer key = 11;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertFalse(embedded.remove(key, "vX"));
      assertTrue(embedded.remove(key, "v1"));
      assertEquals(null, remote.get(key));
   }

   public void testHotRodRemoveConditionalEmbeddedGet() {
      final Integer key = 12;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      VersionedValue<String> versioned = remote.getVersioned(key);
      assertFalse(remote.withFlags(Flag.FORCE_RETURN_VALUE).removeWithVersion(key, Long.MAX_VALUE));
      assertTrue(remote.withFlags(Flag.FORCE_RETURN_VALUE).removeWithVersion(key, versioned.getVersion()));
      assertEquals(null, embedded.get(key));
   }

}
