/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.client.hotrod.stress;

import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Test the performance of ConsistentHashV1/V2.
 *
 * @author Dan Berindei
 * @since 5.3
 */
@Test(groups = "profiling", testName = "client.hotrod.ClientConsistentHashPerfTest")
public class ClientConsistentHashPerfTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 64;
   private static final int ITERATIONS = 10000000;
   private static final int NUM_KEYS = 100000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createHotRodServers(NUM_SERVERS, config);
   }

   public void testConsistentHashPerf() throws Exception {
      RemoteCacheManager rcm = client(0);
      RemoteCache<Object, Object> cache = rcm.getCache();
      // This will initialize the consistent hash
      cache.put("k", "v");

      TcpTransportFactory transportFactory = (TcpTransportFactory) TestingUtil.extractField(rcm, "transportFactory");
      ConsistentHash ch = transportFactory.getConsistentHash();
      byte[][] keys = new byte[NUM_KEYS][];

      for (int i = 0; i < NUM_KEYS; i++) {
         keys[i] = String.valueOf(i).getBytes("UTF-8");
      }

      SocketAddress aServer = null;
      // warm-up
      for (int i = 0; i < ITERATIONS/10; i++) {
         SocketAddress server = ch.getServer(keys[i % keys.length]);
         if (server != null) aServer = server;
      }

      long startNanos = System.nanoTime();
      for (int i = 0; i < ITERATIONS; i++) {
         SocketAddress server = ch.getServer(keys[i % keys.length]);
         if (server != null) aServer = server;
      }
      double duration = System.nanoTime() - startNanos;
      log.infof("Test took %.3f s, average CH lookup was %.3f ns", duration / 1000000000L, duration / ITERATIONS);
   }
}
