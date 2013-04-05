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
package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ServerShutdownTest", groups = "functional")
public class ServerShutdownTest {

   public void testServerShutdownWithConnectedClient() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(
                  hotRodCacheConfiguration())) {
         @Override
         public void call() {
            HotRodServer hotrodServer = TestHelper.startHotRodServer(cm);
            try {
               withRemoteCacheManager(new RemoteCacheManagerCallable(
                     new RemoteCacheManager("localhost", hotrodServer.getPort())) {
                  @Override
                  public void call() {
                     RemoteCache remoteCache = rcm.getCache();
                     remoteCache.put("k","v");
                     assertEquals("v", remoteCache.get("k"));
                  }
               });
            } finally {
               killServers(hotrodServer);
            }
         }
      });
   }

   public void testServerShutdownWithoutConnectedClient() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(
                  hotRodCacheConfiguration())) {
         @Override
         public void call() {
            HotRodServer hotrodServer = TestHelper.startHotRodServer(cm);
            try {
               withRemoteCacheManager(new RemoteCacheManagerCallable(
                     new RemoteCacheManager("localhost", hotrodServer.getPort())) {
                  @Override
                  public void call() {
                     RemoteCache remoteCache = rcm.getCache();
                     remoteCache.put("k","v");
                     assertEquals("v", remoteCache.get("k"));
                  }
               });
            } finally {
               killServers(hotrodServer);
            }
         }
      });
   }

}
