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
package org.infinispan.cdi.test.cache;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.testng.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.testng.annotations.Test;

import javax.inject.Inject;

import static org.infinispan.cdi.test.testutil.Deployments.baseDeployment;
import static org.infinispan.manager.CacheContainer.DEFAULT_CACHE_NAME;
import static org.testng.Assert.assertEquals;

/**
 * Tests that the default cache is available and can be injected with no configuration.
 *
 * @author Pete Muir
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@Test(groups = "functional", testName = "cdi.test.cache.DefaultCacheTest")
public class DefaultCacheTest extends Arquillian {

   @Deployment
   public static Archive<?> deployment() {
      return baseDeployment()
            .addClass(DefaultCacheTest.class);
   }

   @Inject
   private Cache<String, String> cache;

   @Inject
   private AdvancedCache<String, String> advancedCache;

   public void testDefaultCache() {
      // Simple test to make sure the default cache works
      cache.put("pete", "British");
      cache.put("manik", "Sri Lankan");
      assertEquals(cache.get("pete"), "British");
      assertEquals(cache.get("manik"), "Sri Lankan");
      assertEquals(cache.getName(), DEFAULT_CACHE_NAME);
      /*
       * Check that the advanced cache contains the same data as the simple
       * cache. As we can inject either Cache or AdvancedCache, this is double
       * checking that they both refer to the same underlying impl and Seam
       * Clouds isn't returning the wrong thing.
       */
      assertEquals(advancedCache.get("pete"), "British");
      assertEquals(advancedCache.get("manik"), "Sri Lankan");
      assertEquals(advancedCache.getName(), DEFAULT_CACHE_NAME);
   }
}
