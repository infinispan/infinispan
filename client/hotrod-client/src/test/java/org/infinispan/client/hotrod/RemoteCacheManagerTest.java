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

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.Properties;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.RemoteCacheManagerTest", groups = "functional" )
public class RemoteCacheManagerTest extends SingleCacheManagerTest {

   EmbeddedCacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   int port;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      port = hotrodServer.getPort();
      return cacheManager;
   }

   @AfterTest(alwaysRun = true)
   public void release() {
      try {
         if (cacheManager != null) cacheManager.stop();
         if (hotrodServer != null) hotrodServer.stop();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void testNoArgConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager();
      assert remoteCacheManager.isStarted();
      remoteCacheManager.stop();
   }

   public void testBooleanConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      remoteCacheManager.stop();
   }
   
   public void testUrlAndBooleanConstructor() throws Exception {
      URL resource = Thread.currentThread().getContextClassLoader().getResource("empty-config.properties");
      assert resource != null;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(resource, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.config.getProperties().setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesConstructor() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(p);
      assert remoteCacheManager.isStarted();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesAndBooleanConstructor() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(p, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testStringAndBooleanConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager("localhost:"+hotrodServer.getPort(), false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testGetUndefinedCache() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(p, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assert null == remoteCacheManager.getCache("Undefined1234");
   }

   private void assertWorks(RemoteCacheManager remoteCacheManager) {
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
      cache.put("aKey", "aValue");
      assert cache.get("aKey").equals("aValue");
   }
}
