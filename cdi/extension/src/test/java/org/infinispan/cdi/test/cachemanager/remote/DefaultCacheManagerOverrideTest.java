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
package org.infinispan.cdi.test.cachemanager.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.Properties;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the default remote cache manager can be overridden.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cachemanager.remote.DefaultCacheManagerOverrideTest")
public class DefaultCacheManagerOverrideTest extends Arquillian {

   private static final String SERVER_LIST_KEY = "infinispan.client.hotrod.server_list";
   private static final String SERVER_LIST_VALUE = "foo:15444";

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultCacheManagerOverrideTest.class);
   }

   @Inject
   private RemoteCacheManager remoteCacheManager;

   public void testDefaultRemoteCacheManagerOverride() {
      final Properties properties = remoteCacheManager.getProperties();

      assertEquals(properties.getProperty(SERVER_LIST_KEY), SERVER_LIST_VALUE);
   }

   /**
    * The default remote cache manager producer. This producer will override the default remote cache manager producer
    * provided by the CDI extension.
    */
   @Produces
   @ApplicationScoped
   public RemoteCacheManager defaultRemoteCacheManager() {
      final Properties properties = new Properties();
      properties.put(SERVER_LIST_KEY, SERVER_LIST_VALUE);

      return new RemoteCacheManager(properties);
   }
}
