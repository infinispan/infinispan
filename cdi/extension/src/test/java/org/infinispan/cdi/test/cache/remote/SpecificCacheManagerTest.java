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

import org.infinispan.BasicCache;
import org.infinispan.cdi.Remote;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
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
import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Properties;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the default remote cache manager can be overridden.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cache.remote.SpecificCacheManagerTest")
public class SpecificCacheManagerTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(SpecificCacheManagerTest.class);
   }

   private static HotRodServer hotRodServer;
   private static EmbeddedCacheManager embeddedCacheManager;

   @Inject
   @Small
   public BasicCache<String, String> cache;

   @Inject
   @Small
   public RemoteCache<String, String> remoteCache;

   @BeforeTest
   public void beforeMethod() {
      embeddedCacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      embeddedCacheManager.defineConfiguration("small", embeddedCacheManager.getDefaultConfiguration());
      hotRodServer = TestHelper.startHotRodServer(embeddedCacheManager);
   }

   @AfterTest(alwaysRun = true)
   public void afterMethod() {
      embeddedCacheManager.stop();
      hotRodServer.stop();
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

   @Small
   @Produces
   @ApplicationScoped
   public static RemoteCacheManager smallRemoteCacheManager() {
      final Properties properties = new Properties();
      properties.put(SERVER_LIST_KEY, "127.0.0.1:" + hotRodServer.getPort());

      return new RemoteCacheManager(properties);
   }

   @Remote("small")
   @Qualifier
   @Target({TYPE, METHOD, PARAMETER, FIELD})
   @Retention(RUNTIME)
   @Documented
   public static @interface Small {
   }
}
