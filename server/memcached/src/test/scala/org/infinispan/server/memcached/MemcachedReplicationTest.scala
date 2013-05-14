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
package org.infinispan.server.memcached

import org.testng.Assert._
import org.infinispan.test.TestingUtil._
import org.testng.annotations.Test
import java.util.concurrent.TimeUnit
import java.lang.reflect.Method
import net.spy.memcached.CASResponse
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.test.fwk.TestCacheManagerFactory
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.util.ByteArrayEquivalence

/**
 * Tests replicated Infinispan Memcached servers.
 *
 * @author Galder Zamarreño
 * @since 4.1                               §
 */
@Test(groups = Array("functional"), testName = "server.memcached.MemcachedReplicationTest")
class MemcachedReplicationTest extends MemcachedMultiNodeTest {

   protected def createCacheManager(index: Int): EmbeddedCacheManager = {
      val builder = new ConfigurationBuilder
      // When replicating data, old append/prepend byte[] value in the cache
      // is a different instance (but maybe same contents) to the one shipped
      // by the replace command itself, hence for those tests to work, value
      // equivalence needs to be configured to compare byte array contents
      // and not instance reference pointers.
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
             .dataContainer().valueEquivalence(ByteArrayEquivalence.INSTANCE)
      TestCacheManagerFactory.createClusteredCacheManager(builder)
   }

   def testReplicatedSet(m: Method) {
      val f = clients.head.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.get(k(m)), v(m))
   }

   def testReplicatedGetMultipleKeys(m: Method) {
      val f1 = clients.head.set(k(m, "k1-"), 0, v(m, "v1-"))
      val f2 = clients.head.set(k(m, "k2-"), 0, v(m, "v2-"))
      val f3 = clients.head.set(k(m, "k3-"), 0, v(m, "v3-"))
      assertTrue(f1.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertTrue(f2.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertTrue(f3.get(timeout, TimeUnit.SECONDS).booleanValue)
      val keys = List(k(m, "k1-"), k(m, "k2-"), k(m, "k3-"))
      val ret = clients.tail.head.getBulk(keys: _*)
      assertEquals(ret.get(k(m, "k1-")), v(m, "v1-"))
      assertEquals(ret.get(k(m, "k2-")), v(m, "v2-"))
      assertEquals(ret.get(k(m, "k3-")), v(m, "v3-"))
   }

   def testReplicatedAdd(m: Method) {
      val f = clients.head.add(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.get(k(m)), v(m))
   }

   def testReplicatedReplace(m: Method) {
      var f = clients.head.add(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.get(k(m)), v(m))
      f = clients.tail.head.replace(k(m), 0, v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.head.get(k(m)), v(m, "v1-"))
   }

   def testReplicatedAppend(m: Method) {
      var f = clients.head.add(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.get(k(m)), v(m))
      f = clients.tail.head.append(0, k(m), v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val expected = v(m) + v(m, "v1-")
      assertEquals(clients.head.get(k(m)), expected)
   }

   def testReplicatedPrepend(m: Method) {
      var f = clients.head.add(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.get(k(m)), v(m))
      f = clients.tail.head.prepend(0, k(m), v(m, "v1-"))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val expected = v(m, "v1-") + v(m)
      assertEquals(clients.head.get(k(m)), expected)
  }

   def testReplicatedGets(m: Method) {
      val f = clients.head.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      val value = clients.tail.head.gets(k(m))
      assertEquals(value.getValue, v(m))
      assertTrue(value.getCas != 0)
   }

   def testReplicatedCasExists(m: Method) {
      val f = clients.head.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      var value = clients.tail.head.gets(k(m))
      assertEquals(value.getValue, v(m))
      assertTrue(value.getCas != 0)
      val old = value.getCas
      var resp = clients.tail.head.cas(k(m), value.getCas, v(m, "v1-"))
      value = clients.head.gets(k(m))
      assertEquals(value.getValue, v(m, "v1-"))
      assertTrue(value.getCas != 0)
      assertTrue(value.getCas != old)
      resp = clients.head.cas(k(m), old, v(m, "v2-"))
      assertEquals(resp, CASResponse.EXISTS)
      resp = clients.tail.head.cas(k(m), value.getCas, v(m, "v2-"))
      assertEquals(resp, CASResponse.OK)
   }

   def testReplicatedDelete(m: Method) {
      var f = clients.head.set(k(m), 0, v(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      f = clients.tail.head.delete(k(m))
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
   }

   def testReplicatedIncrement(m: Method) {
      val f = clients.head.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.incr(k(m), 1), 2)
   }

   def testReplicatedDecrement(m: Method) {
      val f = clients.head.set(k(m), 0, "1")
      assertTrue(f.get(timeout, TimeUnit.SECONDS).booleanValue)
      assertEquals(clients.tail.head.decr(k(m), 1), 0)
   }

}