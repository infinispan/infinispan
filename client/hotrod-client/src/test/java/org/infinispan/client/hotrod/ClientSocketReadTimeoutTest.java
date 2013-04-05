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

package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.manager.AbstractDelegatingEmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.*;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;

/**
 * This test is used to verify that clients get a timeout when the server does
 * not respond with the requested bytes.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(testName = "client.hotrod.ClientSocketReadTimeoutTest", groups = "functional" )
public class ClientSocketReadTimeoutTest extends SingleCacheManagerTest {

   HotRodServer hotrodServer;
   RemoteCacheManager remoteCacheManager;
   RemoteCache defaultRemote;
   CountDownLatch latch;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      latch = new CountDownLatch(1);
      cacheManager = new HangingCacheManager(
            TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration()),
            latch);
      // cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      // pass the config file to the cache
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();
      defaultRemote = remoteCacheManager.getCache();

      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      config.put("infinispan.client.hotrod.socket_timeout", "5000");
      config.put("infinispan.client.hotrod.connect_timeout", "5000");
      config.put("maxActive", 2);
      // Set ping on startup false, so that the hang can happen
      // when the put comes, and not when the remote cache manager is built.
      config.put("infinispan.client.hotrod.ping_on_startup", "false");
      return new RemoteCacheManager(config);
   }

   @Override
   protected void teardown() {
      latch.countDown();
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      super.teardown();
   }

   @Test(expectedExceptions = SocketTimeoutException.class)
   public void testPutTimeout(Method m) throws Throwable {
      try {
         assert null == defaultRemote.put(k(m), v(m));
      } catch (TransportException e) {
         throw e.getCause();
      }

   }

   private static class HangingCacheManager extends AbstractDelegatingEmbeddedCacheManager {

      static Log log = LogFactory.getLog(HangingCacheManager.class);

      final CountDownLatch latch;

      public HangingCacheManager(EmbeddedCacheManager delegate, CountDownLatch latch) {
         super(delegate);
         this.latch = latch;
      }

      @Override
      public <K, V> Cache<K, V> getCache() {
         log.info("Retrieve cache from hanging cache manager");
         // TODO: Hacky but it's the easiest thing to do - consider ByteMan
         // ByteMan apparently supports testng since 1.5.1 but no clear
         // example out there, with more time it should be considered.
         String threadName = Thread.currentThread().getName();
         if (threadName.startsWith("HotRod")) {
            log.info("Thread is a HotRod server worker thread, so force wait");
            try {
               // Wait a max of 3 minutes, otherwise socket timeout's not working
               latch.await(180, TimeUnit.SECONDS);
               log.info("Wait finished, return the cache");
               return super.getCache();
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new CacheException(e);
            }
         }
         return super.getCache();
      }
      
   }

}
