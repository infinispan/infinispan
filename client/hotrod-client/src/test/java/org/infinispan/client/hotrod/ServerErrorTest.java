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

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Properties;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * Tests HotRod client and server behaivour when server throws a server error
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "client.hotrod.ServerErrorTest")
public class ServerErrorTest extends SingleCacheManagerTest {

   private HotRodServer hotrodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      hotrodServer = TestHelper.startHotRodServer(cacheManager);

      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache();

      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      return new RemoteCacheManager(config);
   }

   @AfterClass
   public void shutDownHotrod() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   public void testErrorWhileDoingPut(Method m) throws Exception {
      cache.addListener(new ErrorInducingListener());
      remoteCache = remoteCacheManager.getCache();

      remoteCache.put(k(m), v(m));
      assert remoteCache.get(k(m)).equals(v(m));

      try {
         remoteCache.put("FailFailFail", "whatever...");
      } catch (HotRodClientException e) {
         // ignore
      }

      try {
         remoteCache.put(k(m, 2), v(m, 2));
         assert remoteCache.get(k(m, 2)).equals(v(m, 2));
      } catch (Exception e) {
         log.error("Error sending request after server failure", e);
         throw e;
      }

   }

   @Listener
   public static class ErrorInducingListener {
      @CacheEntryCreated
      public void entryCreated(CacheEntryEvent event) throws Exception {
         if (event.isPre() && unmarshall(event.getKey()).equals("FailFailFail")) {
            throw new SuspectException("Simulated suspicion");
         }
      }

      private String unmarshall(Object key) throws Exception {
         Marshaller marshaller = new JBossMarshaller();
         return (String) marshaller.objectFromByteBuffer(((ByteArrayKey) key).getData());
      }
   }
}
