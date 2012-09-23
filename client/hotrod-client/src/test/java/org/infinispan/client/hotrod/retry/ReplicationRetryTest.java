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
package org.infinispan.client.hotrod.retry;

import static org.testng.Assert.assertEquals;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.ReplicationRetryTest", groups = "functional")
public class ReplicationRetryTest extends AbstractRetryTest {

   public void testGet() {
      validateSequenceAndStopServer();
      //now make sure that next call won't fail
      resetStats();
      for (int i = 0; i < 100; i++) {
         assert remoteCache.get("k").equals("v");
      }
   }

   public void testPut() {

      validateSequenceAndStopServer();
      resetStats();

      assertEquals(remoteCache.put("k", "v0"), "v");
      for (int i = 1; i < 100; i++) {
         assertEquals("v" + (i-1), remoteCache.put("k", "v"+i));
      }
   }

   public void testRemove() {
      validateSequenceAndStopServer();
      resetStats();

      assertEquals("v", remoteCache.remove("k"));
   }

   public void testContains() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(true, remoteCache.containsKey("k"));
   }

   public void testGetWithVersion() {
      validateSequenceAndStopServer();
      resetStats();
      VersionedValue value = remoteCache.getVersioned("k");
      assertEquals("v", value.getValue());
   }

   public void testPutIfAbsent() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(null, remoteCache.putIfAbsent("noSuchKey", "someValue"));
      assertEquals("someValue", remoteCache.get("noSuchKey"));
   }

   public void testReplace() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals("v", remoteCache.replace("k", "v2"));
   }

   public void testReplaceIfUnmodified() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(false, remoteCache.replaceWithVersion("k", "v2", 12));
   }

   public void testRemoveIfUnmodified() {
      validateSequenceAndStopServer();
      resetStats();
      assertEquals(false, remoteCache.removeWithVersion("k", 12));
   }

   public void testClear() {
      validateSequenceAndStopServer();
      resetStats();
      remoteCache.clear();
      assertEquals(false, remoteCache.containsKey("k"));
   }

   public void testBulkGet() {
      validateSequenceAndStopServer();
      resetStats();
      Map map = remoteCache.getBulk();
      assertEquals(3, map.size());
   }

   private void validateSequenceAndStopServer() {
      resetStats();
      assertNoHits();
      SocketAddress expectedServer = strategy.getServers()[strategy.getNextPosition()];
      assertNoHits();
      remoteCache.put("k","v");

      assert strategy.getServers().length == 3;
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      remoteCache.put("k2","v2");
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      remoteCache.put("k3","v3");
      assertOnlyServerHit(expectedServer);

      resetStats();
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      assertEquals("v", remoteCache.put("k","v"));
      assertOnlyServerHit(expectedServer);

      //this would be the next server to be shutdown
      expectedServer = strategy.getServers()[strategy.getNextPosition()];
      HotRodServer toStop = addr2hrServer.get(expectedServer);
      toStop.stop();
      for (Iterator<EmbeddedCacheManager> ecmIt = cacheManagers.iterator(); ecmIt.hasNext();) {
         if (ecmIt.next().getAddress().equals(expectedServer)) ecmIt.remove();
      }
      waitForClusterToForm();
   }

   @Override
   protected ConfigurationBuilder getCacheConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }
}
