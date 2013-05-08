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
package org.infinispan.cdi.test.cache.remote;

import org.infinispan.api.BasicCache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.Properties;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.client.hotrod.TestHelper.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the use of a specific cache manager for one cache.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cache.remote.SpecificCacheManagerTest")
public class SpecificCacheManagerTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(SpecificCacheManagerTest.class)
            .addClass(Small.class);
   }

   private static HotRodServer hotRodServer;
   private static EmbeddedCacheManager embeddedCacheManager;

   @Inject
   @Small
   private BasicCache<String, String> cache;

   @Inject
   @Small
   private RemoteCache<String, String> remoteCache;

   @BeforeTest
   public void beforeMethod() {
      embeddedCacheManager = TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration(TestCacheManagerFactory
                  .getDefaultCacheConfiguration(false)));
      embeddedCacheManager.defineConfiguration("small", embeddedCacheManager.getDefaultCacheConfiguration());
      hotRodServer = startHotRodServer(embeddedCacheManager);
   }

   @AfterTest(alwaysRun = true)
   public void afterMethod() {
      if (embeddedCacheManager != null) embeddedCacheManager.stop();
      if (hotRodServer != null) hotRodServer.stop();
   }

   public void testSpecificCacheManager() {
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");

      assertEquals(cache.getName(), "small");
      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");

      assertEquals(remoteCache.getName(), "small");
      assertEquals(remoteCache.get("pete"), "British");
      assertEquals(remoteCache.get("manik"), "Sri Lankan");
   }

   /**
    * Produces a specific cache manager for the small cache.
    *
    * @see Small
    */
   @Small
   @Produces
   @ApplicationScoped
   public static RemoteCacheManager smallRemoteCacheManager() {
      return new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
                  .addServers("127.0.0.1:" + hotRodServer.getPort()).build());
   }

}
